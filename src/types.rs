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
// Truncates silently if you feed it something longer, don't do that.
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

    /// Trailing NULs trimmed, lossy on anything that isn't valid UTF-8.
    /// For labels and logging only, never round-trip this back into a
    /// Symbol.
    pub fn as_str(&self) -> &str {
        let end = self.0.iter().position(|&b| b == 0).unwrap_or(self.0.len());
        std::str::from_utf8(&self.0[..end]).unwrap_or("<invalid-symbol>")
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum Exchange {
    #[default]
    Binance     = 0,
    Bybit       = 1,
    Hyperliquid = 2,
}

impl Exchange {
    pub const fn as_str(self) -> &'static str {
        match self {
            Exchange::Binance     => "binance",
            Exchange::Bybit       => "bybit",
            Exchange::Hyperliquid => "hyperliquid",
        }
    }
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

impl NormalizedTick {
    // _align_pad is crate-private so nothing outside sets it by hand, but
    // that also means external crates (integration tests, downstream
    // users) can't build one at all via struct literal, not even with
    // `..Default::default()`. This is the escape hatch.
    #[inline(always)]
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        price: Price,
        qty: Qty,
        sequence: u64,
        ts_exchange_ns: u64,
        ts_recv_ns: u64,
        symbol: Symbol,
        exchange: Exchange,
        side: Side,
        is_snapshot: bool,
        snapshot_id: u32,
    ) -> Self {
        NormalizedTick {
            price, qty, sequence, ts_exchange_ns, ts_recv_ns, symbol, exchange,
            side, is_snapshot, _align_pad: 0, snapshot_id,
        }
    }
}


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


pub const MAX_LEVELS: usize = 64;

// count is on its own cache line so updating it doesn't thrash the levels array.
// learned this the hard way on a 10G feed, saved ~80ns/update
//
// bids: sorted descending. asks: ascending. levels[0] is always best.
//
// TODO: consider a hybrid layout, keep top 5 levels in a separate array for
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
        // ptr::copy is memmove, handles overlap, faster than a loop and the compiler knows it
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
            // book's full but this price beats something we track, evict the worst
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


// bids (1088B) | asks (1088B) | metadata (128B) = 2304B total, 36 cache lines.
// Each side sits on its own cache-line-aligned region. One-sided updates
// don't touch the other side's lines. This matters at 500k updates/sec.
#[repr(C, align(64))]
pub struct OrderBook {
    pub bids:               BookSide,
    pub asks:               BookSide,
    pub sequence:           u64,
    pub last_update_ns:     u64,
    // Observability counters, not correctness gates. apply() never rejects
    // a tick over sequence, resyncing after a gap is the adapter's job.
    pub symbol_mismatches:  u64,
    pub sequence_gaps:      u64,
    pub sequence_reorders:  u64,
    pub checksum_failures:  u64,
    pub bids_snapshot_id:   u32,
    pub asks_snapshot_id:   u32,
    pub symbol:             Symbol,
    pub exchange:           Exchange,
    sequence_initialized:   bool,
    _meta_pad:              [u8; 54], // 8*6+4*2+16+1+1+54 = 128, don't touch
}

const _: () = assert!(mem::size_of::<OrderBook>() == 2304);

impl OrderBook {
    pub fn new(symbol: Symbol, exchange: Exchange) -> Box<Self> {
        Box::new(OrderBook {
            bids:                 BookSide::new(),
            asks:                 BookSide::new(),
            sequence:             0,
            last_update_ns:       0,
            // sentinel, not 0. adapters usually start snapshot_id at 0, and 0 used
            // to be the default here too, so the first real snapshot never cleared
            // anything. u32::MAX only bites if the very first snapshot happens to
            // carry that exact id, which isn't going to happen in practice.
            bids_snapshot_id:     u32::MAX,
            asks_snapshot_id:     u32::MAX,
            symbol,
            exchange,
            symbol_mismatches:    0,
            sequence_gaps:        0,
            sequence_reorders:    0,
            checksum_failures:    0,
            sequence_initialized: false,
            _meta_pad:            [0u8; 54],
        })
    }

    #[inline(always)]
    pub fn apply(&mut self, tick: &NormalizedTick) -> DeltaResult {
        // debug_assert alone used to be the only check here, which means it
        // compiled out entirely in release, exactly the build that runs in
        // prod. A router bug would silently merge one symbol's ticks into
        // another book. Now release still discards and counts it instead of
        // corrupting the book; debug keeps failing loud and immediate.
        if tick.symbol != self.symbol {
            debug_assert!(false, "wrong book, fix your router");
            self.symbol_mismatches += 1;
            return DeltaResult::Discarded;
        }

        if tick.is_snapshot {
            self.maybe_clear_for_snapshot(tick.side, tick.snapshot_id);
        }

        let result = match tick.side {
            Side::Bid => self.bids.apply_delta(tick.price, tick.qty, true),
            Side::Ask => self.asks.apply_delta(tick.price, tick.qty, false),
        };

        if !matches!(result, DeltaResult::NoOp | DeltaResult::Discarded) {
            self.track_sequence(tick.sequence);
            self.last_update_ns = tick.ts_recv_ns;
        }

        result
    }

    // Counts gaps (missed packets) and reorders (duplicate or out-of-order)
    // separately. Doesn't reject either, resync is somebody else's problem,
    // this just makes sure it's visible instead of silently absorbed. First
    // tick after construction never counts, there's nothing to compare yet.
    #[inline(always)]
    fn track_sequence(&mut self, seq: u64) {
        if self.sequence_initialized {
            if seq > self.sequence {
                if seq != self.sequence + 1 {
                    self.sequence_gaps += 1;
                }
            } else {
                self.sequence_reorders += 1;
            }
        } else {
            self.sequence_initialized = true;
        }
        self.sequence = seq;
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

    /// True when the best bid trades through the best ask. `saturating_sub` in
    /// `spread()` clamps that case to 0, which reads the same as "tight
    /// market" unless you check here too. Bad snapshot, partial state
    /// mid-reconnect, race between two update streams, whatever the cause,
    /// this is the thing you actually want an alert on.
    #[inline(always)]
    pub fn is_crossed(&self) -> bool {
        match (self.bids.best(), self.asks.best()) {
            (Some(bid), Some(ask)) => bid.price.0 > ask.price.0,
            _ => false,
        }
    }

    /// True if nothing has updated this book within `threshold_ns` of
    /// `now_ns`. A book that's never been touched (`last_update_ns` == 0)
    /// falls out of this naturally, `now_ns` will be far past any sane
    /// threshold. `now_ns` needs to be the same clock domain as the
    /// `ts_recv_ns` your adapter stamps ticks with.
    #[inline(always)]
    pub fn is_stale(&self, now_ns: u64, threshold_ns: u64) -> bool {
        now_ns.saturating_sub(self.last_update_ns) > threshold_ns
    }

    /// Feeds (side, price, qty) for the top `n` levels per side to `f`, best
    /// price first, same order the book already stores them in. This is as
    /// far as this crate goes toward checksum validation: Bybit and OKX ship
    /// a checksum with their L2 deltas, but the concatenation format and the
    /// CRC32 seed are exchange-specific and undocumented here, so that math
    /// belongs at the adapter layer, not guessed at in this crate. Once your
    /// adapter has verified (or failed) the checksum, call
    /// `record_checksum_failure()` to keep the result with the book's other
    /// health counters instead of scattered across adapter code.
    pub fn for_checksum<F: FnMut(Side, Price, Qty)>(&self, n: usize, mut f: F) {
        for lvl in self.bids.levels.iter().take(n.min(self.bids.count)) {
            f(Side::Bid, lvl.price, lvl.qty);
        }
        for lvl in self.asks.levels.iter().take(n.min(self.asks.count)) {
            f(Side::Ask, lvl.price, lvl.qty);
        }
    }

    #[inline(always)]
    pub fn record_checksum_failure(&mut self) {
        self.checksum_failures += 1;
    }
}
