use crate::{
    ring_buffer::SpscDisruptor,
    timer,
    types::{DeltaResult, NormalizedTick, OrderBook},
};
use std::sync::Arc;

// Latency sample. Caller owns the arena, we just write into a slot.
// Keep this repr(C) so it's safe to dump directly into a ring buffer or mmap.
#[derive(Debug, Clone, Copy, Default)]
#[repr(C)]
pub struct TickStats {
    pub latency_ticks: u64,
    pub sequence:      u64,
    pub result:        DeltaResult,
    _pad:              [u8; 7],
}

// One engine per instrument per exchange. If you need multi-book routing,
// spin up multiple engines and let the strategy layer decide which one to query.
// TODO: add a router layer that fans out one ring to N books keyed by symbol,
// right now callers have to do that themselves.
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

        let t0 = stats_out.as_ref().map_or(0, |_| timer::rdtsc());
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
        assert!(unsafe { ring.try_publish(t) }, "ring full in test, bump RING_SIZE?");
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

    // Regression: bids_snapshot_id/asks_snapshot_id used to default to 0, so a
    // snapshot whose own id happened to be 0 was indistinguishable from "no
    // snapshot applied yet" and never cleared stale levels.
    #[test]
    fn snapshot_id_zero_still_clears_stale_book() {
        let (mut eng, ring) = make_engine();
        push(&ring, make_tick(999, 1_0000_0000, Side::Bid, 1));
        eng.run_batch();
        assert_eq!(eng.book().bids.count, 1);

        push(&ring, snap_tick(100, 5_0000_0000, Side::Bid, 2, 0));
        eng.run_batch();

        assert_eq!(eng.book().bids.count, 1);
        assert_eq!(eng.book().bids.levels[0].price.raw(), 100);
    }

    // Regression: the symbol check used to be a bare debug_assert_eq!, which
    // compiles out entirely in release. A tick for the wrong book used to get
    // applied anyway with no signal at all. Debug builds still fail loud and
    // fast (below); release must discard and count it instead of panicking
    // or, worse, silently applying it to the wrong book.
    #[cfg(debug_assertions)]
    #[test]
    #[should_panic(expected = "wrong book")]
    fn wrong_symbol_tick_panics_in_debug() {
        let (mut eng, ring) = make_engine();
        let wrong_symbol = NormalizedTick {
            symbol: Symbol::from_bytes(b"ETHUSDT"),
            ..make_tick(100, 1_0000_0000, Side::Bid, 1)
        };
        push(&ring, wrong_symbol);
        eng.run_batch();
    }

    #[cfg(not(debug_assertions))]
    #[test]
    fn wrong_symbol_tick_is_discarded_not_applied() {
        let (mut eng, ring) = make_engine();
        let wrong_symbol = NormalizedTick {
            symbol: Symbol::from_bytes(b"ETHUSDT"),
            ..make_tick(100, 1_0000_0000, Side::Bid, 1)
        };
        push(&ring, wrong_symbol);
        eng.run_batch();

        assert_eq!(eng.book().bids.count, 0, "wrong-symbol tick must not touch the book");
        assert_eq!(eng.book().symbol_mismatches, 1);
    }

    // Regression: spread() clamps a crossed book to 0 via saturating_sub,
    // which looks identical to a tight, healthy market. is_crossed() is the
    // dedicated check for the case spread() can't distinguish.
    #[test]
    fn crossed_book_detected_separately_from_spread() {
        let (mut eng, ring) = make_engine();
        push(&ring, make_tick(10100, 1_0000_0000, Side::Bid, 1));
        push(&ring, make_tick(9900, 1_0000_0000, Side::Ask, 2));
        eng.run_batch();

        assert_eq!(eng.book().spread(), Some(0));
        assert!(eng.book().is_crossed());
    }

    #[test]
    fn tight_book_is_not_crossed() {
        let (mut eng, ring) = make_engine();
        push(&ring, make_tick(9900, 1_0000_0000, Side::Bid, 1));
        push(&ring, make_tick(10100, 1_0000_0000, Side::Ask, 2));
        eng.run_batch();

        assert!(!eng.book().is_crossed());
    }

    // Regression: nothing tracked missed or reordered sequence numbers.
    // A dropped packet or a stream replaying out of order applied silently.
    #[test]
    fn sequence_gap_is_counted() {
        let (mut eng, ring) = make_engine();
        push(&ring, make_tick(100, 1_0000_0000, Side::Bid, 1));
        push(&ring, make_tick(101, 1_0000_0000, Side::Bid, 5)); // skipped 2,3,4
        eng.run_batch();

        assert_eq!(eng.book().sequence_gaps, 1);
        assert_eq!(eng.book().sequence_reorders, 0);
    }

    #[test]
    fn out_of_order_sequence_is_counted_as_reorder_not_gap() {
        let (mut eng, ring) = make_engine();
        push(&ring, make_tick(100, 1_0000_0000, Side::Bid, 5));
        push(&ring, make_tick(101, 1_0000_0000, Side::Bid, 3)); // went backward
        eng.run_batch();

        assert_eq!(eng.book().sequence_reorders, 1);
        assert_eq!(eng.book().sequence_gaps, 0);
    }

    #[test]
    fn contiguous_sequence_counts_neither() {
        let (mut eng, ring) = make_engine();
        for seq in 1..=5u64 {
            push(&ring, make_tick(100 + seq, 1_0000_0000, Side::Bid, seq));
        }
        eng.run_batch();

        assert_eq!(eng.book().sequence_gaps, 0);
        assert_eq!(eng.book().sequence_reorders, 0);
    }

    // The very first tick ever applied has nothing to compare against and
    // must never be miscounted as a gap just because sequence started at 0.
    #[test]
    fn first_tick_ever_is_never_a_gap() {
        let (mut eng, ring) = make_engine();
        push(&ring, make_tick(100, 1_0000_0000, Side::Bid, 500));
        eng.run_batch();

        assert_eq!(eng.book().sequence_gaps, 0);
        assert_eq!(eng.book().sequence_reorders, 0);
    }

    #[test]
    fn fresh_update_is_not_stale() {
        let (mut eng, ring) = make_engine();
        let t = NormalizedTick { ts_recv_ns: 1_000_000_000, ..make_tick(100, 1_0000_0000, Side::Bid, 1) };
        push(&ring, t);
        eng.run_batch();

        assert!(!eng.book().is_stale(1_000_000_500, 1_000)); // 500ns later, well under a 1us threshold
    }

    #[test]
    fn book_past_threshold_is_stale() {
        let (mut eng, ring) = make_engine();
        let t = NormalizedTick { ts_recv_ns: 1_000_000_000, ..make_tick(100, 1_0000_0000, Side::Bid, 1) };
        push(&ring, t);
        eng.run_batch();

        assert!(eng.book().is_stale(2_000_000_000, 1_000)); // 1 full second later, way past a 1us threshold
    }

    #[test]
    fn never_updated_book_is_stale() {
        let book = OrderBook::new(Symbol::from_bytes(b"BTCUSDT"), Exchange::Binance);
        assert!(book.is_stale(1_000_000_000, 1_000));
    }

    #[test]
    fn for_checksum_yields_levels_best_first_bids_then_asks() {
        let (mut eng, ring) = make_engine();
        for (p, s) in [(99u64, 1u64), (101, 2), (100, 3)] {
            push(&ring, make_tick(p, 1_0000_0000, Side::Bid, s));
        }
        for (p, s) in [(103u64, 4u64), (102, 5)] {
            push(&ring, make_tick(p, 1_0000_0000, Side::Ask, s));
        }
        eng.run_batch();

        let mut seen = Vec::new();
        eng.book().for_checksum(2, |side, price, _qty| seen.push((side, price.raw())));

        assert_eq!(seen, vec![
            (Side::Bid, 101), (Side::Bid, 100), // top 2 bids, best first, third excluded
            (Side::Ask, 102), (Side::Ask, 103), // top 2 asks, best first
        ]);
    }

    #[test]
    fn checksum_failure_is_counted() {
        let mut book = OrderBook::new(Symbol::from_bytes(b"BTCUSDT"), Exchange::Binance);
        assert_eq!(book.checksum_failures, 0);
        book.record_checksum_failure();
        book.record_checksum_failure();
        assert_eq!(book.checksum_failures, 2);
    }
}
