use std::mem;

// Price and qty are fixed-point, 1e-8 scale. Picked 8 decimals because it
// covers everything from BTC to shitcoins without going above u64 max.
// If someone needs options pricing with fractional ticks... future problem.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
#[repr(transparent)]
pub struct Price(pub u64);

impl Price {
    #[inline(always)] pub const fn new(raw: u64) -> Self { Price(raw) }
    #[inline(always)] pub const fn raw(self) -> u64 { self.0 }
    #[inline(always)] pub const fn is_zero(self) -> bool { self.0 == 0 }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
#[repr(transparent)]
pub struct Qty(pub u64);

impl Qty {
    #[inline(always)] pub const fn new(raw: u64) -> Self { Qty(raw) }
    #[inline(always)] pub const fn raw(self) -> u64 { self.0 }
    #[inline(always)] pub const fn is_zero(self) -> bool { self.0 == 0 }
}

// 16 bytes fits in a 128-bit register, no heap, easy to copy around.
// Truncates silently if you feed it something longer — don't do that.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
#[repr(transparent)]
pub struct Symbol([u8; 16]);

impl Symbol {
    pub fn from_bytes(b: &[u8]) -> Self {
        let mut arr = [0u8; 16];
        arr[..b.len().min(16)].copy_from_slice(&b[..b.len().min(16)]);
        Symbol(arr)
    }

    #[inline(always)]
    pub fn as_bytes(&self) -> &[u8; 16] { &self.0 }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum Exchange {
    #[default]
    Binance     = 0,
    Bybit       = 1,
    Hyperliquid = 2,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum Side {
    #[default] Bid = 0,
    Ask = 1,
}

// NormalizedTick: exactly 64 bytes, one cache line. Yes I counted.
// If you add a field and break this you'll know immediately from the assert below.
//
// Snapshot batching: adapter stamps every tick in a snapshot batch with
// is_snapshot=true + the same snapshot_id. apply() clears the side only when
// the id changes, so the whole batch lands in the book intact. The old version
// cleared on every tick and you'd end up with one level. Don't revert that.
//
// TODO: snapshot_id is u32 and will wrap at ~4B snapshots. In practice that's
// years of runtime, but worth revisiting if we ever replay historical data fast.
#[derive(Debug, Clone, Copy, Default)]
#[repr(C, align(64))]
pub struct NormalizedTick {
    pub price:          Price,
    pub qty:            Qty,
    pub sequence:       u64,
    pub ts_exchange_ns: u64,
    pub ts_recv_ns:     u64,
    pub symbol:         Symbol,
    pub exchange:       Exchange,
    pub side:           Side,
    pub is_snapshot:    bool,
    pub(crate) _align_pad: u8, // keeps snapshot_id 4-aligned, not a mistake
    pub snapshot_id:    u32,
}

const _: () = assert!(mem::size_of::<NormalizedTick>() == 64);
const _: () = assert!(mem::align_of::<NormalizedTick>() == 64);

// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug, Clone, Copy, Default)]
#[repr(C)]
pub struct Level {
    pub price: Price,
    pub qty:   Qty,
}

impl Level {
    #[inline(always)]
    pub const fn empty() -> Self { Level { price: Price(0), qty: Qty(0) } }
}

const _: () = assert!(mem::size_of::<Level>() == 16);

// ─────────────────────────────────────────────────────────────────────────────

pub const MAX_LEVELS: usize = 64;

// count is on its own cache line so updating it doesn't thrash the levels array.
// Learned this one the hard way on a 10G feed — saved ~80ns per update.
//
// bids: sorted descending. asks: ascending. levels[0] is always best.
//
// TODO: consider a hybrid layout — keep top 5 levels in a separate array for
// BBO queries that don't need to touch the rest. Premature for now.
#[repr(C, align(64))]
pub struct BookSide {
    pub count:  usize,
    _pad:       [u8; 56],
    pub levels: [Level; MAX_LEVELS],
}

const _: () = assert!(mem::size_of::<BookSide>() == 1088);

impl BookSide {
    pub const fn new() -> Self {
        BookSide {
            count:  0,
            _pad:   [0u8; 56],
            levels: [Level { price: Price(0), qty: Qty(0) }; MAX_LEVELS],
        }
    }

    #[inline(always)]
    pub fn clear(&mut self) {
        self.count = 0; // stale data past count is harmless, don't waste time zeroing
    }

    #[inline(always)]
    pub fn best(&self) -> Option<Level> {
        (self.count > 0).then_some(self.levels[0])
    }

    #[inline(always)]
    pub fn apply_delta(&mut self, price: Price, qty: Qty, descending: bool) -> DeltaResult {
        let pos = self.search(price, descending);
        if qty.is_zero() {
            match pos {
                Ok(idx) => self.remove(idx),
                Err(_)  => DeltaResult::NoOp, // cancel for a level we don't track, fine
            }
        } else {
            match pos {
                Ok(idx)  => { self.levels[idx].qty = qty; DeltaResult::Updated }
                Err(idx) => self.insert(idx, Level { price, qty }),
            }
        }
    }

    #[inline(always)]
    fn search(&self, price: Price, descending: bool) -> Result<usize, usize> {
        let slice = &self.levels[..self.count];
        if descending {
            // comparator reversal: price.cmp(&elem) gives correct ordering for desc-sorted slice
            slice.binary_search_by(|l| price.cmp(&l.price))
        } else {
            slice.binary_search_by_key(&price, |l| l.price)
        }
    }

    #[inline(always)]
    fn remove(&mut self, idx: usize) -> DeltaResult {
        let tail = self.count - 1;
        // ptr::copy is memmove — handles overlap, faster than a loop, compiler knows it too
        unsafe {
            let p = self.levels.as_mut_ptr().add(idx);
            std::ptr::copy(p.add(1), p, tail - idx);
        }
        self.count = tail;
        DeltaResult::Removed
    }

    #[inline(always)]
    fn insert(&mut self, idx: usize, level: Level) -> DeltaResult {
        if self.count < MAX_LEVELS {
            unsafe {
                let p = self.levels.as_mut_ptr().add(idx);
                std::ptr::copy(p, p.add(1), self.count - idx);
            }
            self.levels[idx] = level;
            self.count += 1;
            DeltaResult::Inserted
        } else if idx < MAX_LEVELS {
            // book full but this price beats something we're tracking — evict the worst
            unsafe {
                let p = self.levels.as_mut_ptr().add(idx);
                std::ptr::copy(p, p.add(1), MAX_LEVELS - idx - 1);
            }
            self.levels[idx] = level;
            DeltaResult::InsertedEvicted
        } else {
            DeltaResult::Discarded // worse than everything we track, don't care
        }
    }
}

impl Default for BookSide {
    fn default() -> Self { Self::new() }
}

// Granular enough to be useful for debugging, cheap enough to keep around.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum DeltaResult {
    Updated         = 0,
    Inserted        = 1,
    Removed         = 2,
    InsertedEvicted = 3,
    #[default]
    NoOp            = 4,
    Discarded       = 5,
}

// ─────────────────────────────────────────────────────────────────────────────

// bids (1088B) | asks (1088B) | metadata (64B) = 2240B total, 35 cache lines.
// Each side sits on its own cache-line-aligned region. One-sided updates
// don't touch the other side's lines. This matters at 500k updates/sec.
#[repr(C, align(64))]
pub struct OrderBook {
    pub bids:             BookSide,
    pub asks:             BookSide,
    pub sequence:         u64,
    pub last_update_ns:   u64,
    pub bids_snapshot_id: u32,
    pub asks_snapshot_id: u32,
    pub symbol:           Symbol,
    pub exchange:         Exchange,
    _meta_pad:            [u8; 23], // 8+8+4+4+16+1+23 = 64, don't touch
}

const _: () = assert!(mem::size_of::<OrderBook>() == 2240);

impl OrderBook {
    pub fn new(symbol: Symbol, exchange: Exchange) -> Box<Self> {
        Box::new(OrderBook {
            bids:             BookSide::new(),
            asks:             BookSide::new(),
            sequence:         0,
            last_update_ns:   0,
            bids_snapshot_id: 0,
            asks_snapshot_id: 0,
            symbol,
            exchange,
            _meta_pad:        [0u8; 23],
        })
    }

    #[inline(always)]
    pub fn apply(&mut self, tick: &NormalizedTick) -> DeltaResult {
        debug_assert_eq!(tick.symbol, self.symbol, "wrong book, fix your router");

        if tick.is_snapshot {
            self.maybe_clear_for_snapshot(tick.side, tick.snapshot_id);
        }

        let result = match tick.side {
            Side::Bid => self.bids.apply_delta(tick.price, tick.qty, true),
            Side::Ask => self.asks.apply_delta(tick.price, tick.qty, false),
        };

        if !matches!(result, DeltaResult::NoOp | DeltaResult::Discarded) {
            self.sequence = tick.sequence;
            self.last_update_ns = tick.ts_recv_ns;
        }

        result
    }

    // Only clears on the first tick of a new snapshot batch. Every tick in
    // the batch shares the same snapshot_id, so we clear exactly once.
    #[inline(always)]
    fn maybe_clear_for_snapshot(&mut self, side: Side, id: u32) {
        match side {
            Side::Bid if id != self.bids_snapshot_id => {
                self.bids.clear();
                self.bids_snapshot_id = id;
            }
            Side::Ask if id != self.asks_snapshot_id => {
                self.asks.clear();
                self.asks_snapshot_id = id;
            }
            _ => {}
        }
    }

    #[inline(always)]
    pub fn spread(&self) -> Option<u64> {
        let bid = self.bids.best()?.price.0;
        let ask = self.asks.best()?.price.0;
        Some(ask.saturating_sub(bid))
    }
}
