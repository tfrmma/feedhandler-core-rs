use crate::{
    ring_buffer::SpscDisruptor,
    timer,
    types::{DeltaResult, NormalizedTick, OrderBook},
};
use std::sync::Arc;

// Latency sample. Caller owns the arena — we just write into a slot.
// Keep this repr(C) so it's safe to dump directly into a ring buffer or mmap.
#[derive(Debug, Clone, Copy, Default)]
#[repr(C)]
pub struct TickStats {
    pub latency_ticks: u64,
    pub sequence:      u64,
    pub result:        DeltaResult,
    _pad:              [u8; 7],
}

// ─────────────────────────────────────────────────────────────────────────────

// One engine per instrument per exchange. If you need multi-book routing,
// spin up multiple engines and let the strategy layer decide which one to query.
// TODO: add a router layer that fans out one ring to N books keyed by symbol —
// currently callers have to do that themselves.
pub struct BookEngine {
    book:       Box<OrderBook>,
    ring:       Arc<SpscDisruptor>,
    pub ticks:  u64,
}

impl BookEngine {
    pub fn new(book: Box<OrderBook>, ring: Arc<SpscDisruptor>) -> Self {
        BookEngine { book, ring, ticks: 0 }
    }

    /// Process one tick. Pass a stats slot if you're doing latency triage,
    /// leave it None in the normal hot loop (saves the rdtsc pair).
    #[inline(always)]
    pub fn run_one(&mut self, stats_out: Option<&mut TickStats>) -> Option<DeltaResult> {
        let tick: NormalizedTick = unsafe { self.ring.try_consume() }?; // SAFETY: single-consumer

        let t0 = stats_out.as_ref().map(|_| timer::rdtsc()).unwrap_or(0);
        let result = self.book.apply(&tick);

        if let Some(slot) = stats_out {
            *slot = TickStats {
                latency_ticks: timer::rdtsc().wrapping_sub(t0),
                sequence:      tick.sequence,
                result,
                _pad:          [0; 7],
            };
        }

        self.ticks = self.ticks.wrapping_add(1);
        Some(result)
    }

    /// Drain everything pending. This is the tight loop you run on the pinned core.
    #[inline(always)]
    pub fn run_batch(&mut self) -> u32 {
        let mut n = 0u32;
        while let Some(tick) = unsafe { self.ring.try_consume() } { // SAFETY: single-consumer
            self.book.apply(&tick);
            n = n.wrapping_add(1);
        }
        self.ticks = self.ticks.wrapping_add(n as u64);
        n
    }

    #[inline(always)] pub fn book(&self)         -> &OrderBook      { &self.book }
    #[inline(always)] pub fn book_mut(&mut self) -> &mut OrderBook  { &mut self.book }
}

// ─────────────────────────────────────────────────────────────────────────────

// log2-bucketed histogram, fully stack-allocated. Not pretty but it's
// good enough for a quick latency triage without pulling in a metrics crate.
// If you need percentiles at scale, pipe TickStats into something real.
pub struct LatencyHistogram<const N: usize> {
    buckets: [u64; N],
    total:   u64,
    max:     u64,
}

impl<const N: usize> LatencyHistogram<N> {
    pub const fn new() -> Self {
        LatencyHistogram { buckets: [0u64; N], total: 0, max: 0 }
    }

    #[inline(always)]
    pub fn record(&mut self, ticks: u64) {
        let b = if ticks == 0 { 0 } else { (63 - ticks.leading_zeros() as usize).min(N - 1) };
        self.buckets[b] = self.buckets[b].wrapping_add(1);
        self.total = self.total.wrapping_add(1);
        if ticks > self.max { self.max = ticks; }
    }

    pub fn print_summary(&self) {
        println!("latency: {} samples, max {:.1} µs",
            self.total,
            timer::ticks_to_ns(self.max) as f64 / 1_000.0);
        for (i, &count) in self.buckets.iter().enumerate() {
            if count == 0 { continue; }
            let lo = if i == 0 { 0 } else { timer::ticks_to_ns(1u64 << (i - 1)) };
            let hi = timer::ticks_to_ns(1u64 << i);
            println!("  [{:>6}ns, {:>6}ns): {:>10}  ({:.2}%)",
                lo, hi, count, 100.0 * count as f64 / self.total as f64);
        }
    }
}

impl<const N: usize> Default for LatencyHistogram<N> {
    fn default() -> Self { Self::new() }
}

// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Exchange, NormalizedTick, Price, Qty, Side, Symbol};

    fn make_tick(price: u64, qty: u64, side: Side, seq: u64) -> NormalizedTick {
        NormalizedTick {
            price:          Price::new(price),
            qty:            Qty::new(qty),
            sequence:       seq,
            ts_exchange_ns: 0,
            ts_recv_ns:     timer::now_ns(),
            symbol:         Symbol::from_bytes(b"BTCUSDT"),
            exchange:       Exchange::Binance,
            side,
            is_snapshot:    false,
            _align_pad:     0,
            snapshot_id:    0,
        }
    }

    fn snap_tick(price: u64, qty: u64, side: Side, seq: u64, snap_id: u32) -> NormalizedTick {
        NormalizedTick { is_snapshot: true, snapshot_id: snap_id, ..make_tick(price, qty, side, seq) }
    }

    fn make_engine() -> (BookEngine, Arc<SpscDisruptor>) {
        let ring = Arc::new(SpscDisruptor::new());
        let book = OrderBook::new(Symbol::from_bytes(b"BTCUSDT"), Exchange::Binance);
        (BookEngine::new(book, Arc::clone(&ring)), ring)
    }

    fn push(ring: &SpscDisruptor, t: NormalizedTick) {
        assert!(unsafe { ring.try_publish(t) }, "ring full in test — bump RING_SIZE?");
    }

    #[test]
    fn bid_insert_roundtrip() {
        let (mut eng, ring) = make_engine();
        push(&ring, make_tick(100_000_0000_0000, 1_0000_0000, Side::Bid, 1));
        assert_eq!(eng.run_one(None), Some(DeltaResult::Inserted));
        assert_eq!(eng.book().bids.count, 1);
        assert_eq!(eng.book().bids.levels[0].price, Price::new(100_000_0000_0000));
    }

    #[test]
    fn bids_sorted_descending() {
        let (mut eng, ring) = make_engine();
        for (p, s) in [(99u64, 1u64), (101, 2), (100, 3)] {
            push(&ring, make_tick(p, 1_0000_0000, Side::Bid, s));
        }
        eng.run_batch();
        let b = &eng.book().bids;
        assert_eq!(
            [b.levels[0].price.raw(), b.levels[1].price.raw(), b.levels[2].price.raw()],
            [101, 100, 99]
        );
    }

    #[test]
    fn asks_sorted_ascending() {
        let (mut eng, ring) = make_engine();
        for (p, s) in [(102u64, 1u64), (100, 2), (101, 3)] {
            push(&ring, make_tick(p, 1_0000_0000, Side::Ask, s));
        }
        eng.run_batch();
        let a = &eng.book().asks;
        assert_eq!(
            [a.levels[0].price.raw(), a.levels[1].price.raw(), a.levels[2].price.raw()],
            [100, 101, 102]
        );
    }

    #[test]
    fn remove_by_zero_qty() {
        let (mut eng, ring) = make_engine();
        push(&ring, make_tick(100, 1_0000_0000, Side::Bid, 1));
        push(&ring, make_tick(100, 0, Side::Bid, 2));
        eng.run_batch();
        assert_eq!(eng.book().bids.count, 0);
    }

    #[test]
    fn snapshot_batch_all_levels_retained() {
        let (mut eng, ring) = make_engine();

        for i in 0..5u64 {
            push(&ring, make_tick(100 + i, 1_0000_0000, Side::Bid, i));
        }
        eng.run_batch();
        assert_eq!(eng.book().bids.count, 5);

        // All three have snapshot_id=1. First one clears, rest just insert.
        // Regression: old code cleared on every tick so you'd end up with 1 level.
        for (i, p) in [200u64, 199, 198].into_iter().enumerate() {
            push(&ring, snap_tick(p, 5_0000_0000, Side::Bid, 10 + i as u64, 1));
        }
        eng.run_batch();

        assert_eq!(eng.book().bids.count, 3);
        assert_eq!(eng.book().bids.levels[0].price.raw(), 200);
        assert_eq!(eng.book().bids.levels[1].price.raw(), 199);
        assert_eq!(eng.book().bids.levels[2].price.raw(), 198);
    }

    #[test]
    fn second_snapshot_replaces_first() {
        let (mut eng, ring) = make_engine();
        for (i, p) in [200u64, 199].into_iter().enumerate() {
            push(&ring, snap_tick(p, 1_0000_0000, Side::Bid, i as u64, 1));
        }
        eng.run_batch();
        assert_eq!(eng.book().bids.count, 2);

        push(&ring, snap_tick(205, 2_0000_0000, Side::Bid, 10, 2));
        eng.run_batch();
        assert_eq!(eng.book().bids.count, 1);
        assert_eq!(eng.book().bids.levels[0].price.raw(), 205);
    }

    #[test]
    fn spread_calculation() {
        let (mut eng, ring) = make_engine();
        push(&ring, make_tick(9900, 1_0000_0000, Side::Bid, 1));
        push(&ring, make_tick(10100, 1_0000_0000, Side::Ask, 2));
        eng.run_batch();
        assert_eq!(eng.book().spread(), Some(200));
    }
}
