use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use feedhandler::{
    BookEngine, DeltaResult, Exchange, NormalizedTick, OrderBook, Price, Qty, Side,
    SpscDisruptor, Symbol,
};
use std::sync::Arc;

const SYM: Symbol = {
    let mut arr = [0u8; 16];
    arr[0] = b'B'; arr[1] = b'T'; arr[2] = b'C';
    arr[3] = b'U'; arr[4] = b'S'; arr[5] = b'D'; arr[6] = b'T';
    Symbol(arr)
};

fn btcusdt_book() -> Box<OrderBook> {
    OrderBook::new(SYM, Exchange::Binance)
}

fn delta(price: u64, qty: u64, side: Side, seq: u64) -> NormalizedTick {
    NormalizedTick {
        price:          Price::new(price),
        qty:            Qty::new(qty),
        sequence:       seq,
        ts_exchange_ns: 0,
        ts_recv_ns:     0,
        symbol:         SYM,
        exchange:       Exchange::Binance,
        side,
        is_snapshot:    false,
        _align_pad:     0,
        snapshot_id:    0,
    }
}

fn seed_bids(book: &mut OrderBook, n: usize) {
    let base = 100_000_00_000_000u64;
    for i in 0..n {
        book.apply(&delta(base - i as u64 * 1_000_000, 1_00_000_000, Side::Bid, i as u64));
    }
}

fn seed_asks(book: &mut OrderBook, n: usize) {
    let base = 100_001_00_000_000u64;
    for i in 0..n {
        book.apply(&delta(base + i as u64 * 1_000_000, 1_00_000_000, Side::Ask, i as u64));
    }
}

// ── Ring buffer ───────────────────────────────────────────────────────────────

fn bench_ring_roundtrip(c: &mut Criterion) {
    let ring = Arc::new(SpscDisruptor::new());
    let t    = delta(100_000_00_000_000, 1_00_000_000, Side::Bid, 1);

    c.bench_function("ring/roundtrip", |b| {
        b.iter(|| unsafe {
            let _ = ring.try_publish(black_box(t));
            black_box(ring.try_consume())
        });
    });
}

// ── Book update ───────────────────────────────────────────────────────────────

fn bench_book_update(c: &mut Criterion) {
    let mut group = c.benchmark_group("book");

    // Update existing mid-book level (most common path in streaming L2).
    {
        let mut book   = *btcusdt_book();
        seed_bids(&mut book, 32);
        let mid        = 100_000_00_000_000u64 - 16 * 1_000_000;
        let upd        = delta(mid, 2_00_000_000, Side::Bid, 999);

        group.bench_function("update_existing", |b| {
            b.iter(|| black_box(book.apply(black_box(&upd))));
        });
    }

    // Insert a new level (forces a memmove of tail).
    {
        let mut book   = *btcusdt_book();
        seed_bids(&mut book, 32);
        let new_price  = 100_000_00_000_000u64 - 16 * 1_000_000 + 500_000;
        let ins        = delta(new_price, 1_50_000_000, Side::Bid, 1_000);
        let del        = delta(new_price, 0,            Side::Bid, 1_001);

        group.bench_function("insert_new_level", |b| {
            b.iter(|| {
                let r = black_box(book.apply(black_box(&ins)));
                if r == DeltaResult::Inserted { book.apply(&del); }
            });
        });
    }

    // Remove a mid-book level.
    {
        let mut book   = *btcusdt_book();
        seed_bids(&mut book, 32);
        let mid        = 100_000_00_000_000u64 - 16 * 1_000_000;
        let del        = delta(mid, 0,            Side::Bid, 2_000);
        let re_add     = delta(mid, 1_00_000_000, Side::Bid, 2_001);

        group.bench_function("remove_mid", |b| {
            b.iter(|| {
                black_box(book.apply(black_box(&del)));
                black_box(book.apply(black_box(&re_add)));
            });
        });
    }

    group.finish();
}

// ── Full pipeline ─────────────────────────────────────────────────────────────

fn bench_pipeline_single(c: &mut Criterion) {
    let ring    = Arc::new(SpscDisruptor::new());
    let mut eng = BookEngine::new(btcusdt_book(), Arc::clone(&ring));
    let t       = delta(100_001_00_000_000, 1_00_000_000, Side::Ask, 0);

    c.bench_function("pipeline/single_tick", |b| {
        b.iter(|| unsafe {
            ring.try_publish(black_box(t));
            eng.run_one(None);
        });
    });
}

fn bench_pipeline_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("pipeline");

    for batch_size in [4u64, 16, 64, 256] {
        group.throughput(Throughput::Elements(batch_size));
        group.bench_with_input(BenchmarkId::new("batch", batch_size), &batch_size, |b, &n| {
            let ring    = Arc::new(SpscDisruptor::new());
            let mut eng = BookEngine::new(btcusdt_book(), Arc::clone(&ring));

            b.iter(|| unsafe {
                for i in 0..n {
                    let (side, base) = if i & 1 == 0 {
                        (Side::Bid, 100_000_00_000_000u64.saturating_sub(i * 1_000_000))
                    } else {
                        (Side::Ask, 100_001_00_000_000u64.saturating_add(i * 1_000_000))
                    };
                    ring.try_publish(delta(base, 1_00_000_000, side, i));
                }
                black_box(eng.run_batch());
            });
        });
    }

    group.finish();
}

criterion_group!(
    name    = ring;
    config  = Criterion::default().sample_size(1_000);
    targets = bench_ring_roundtrip
);
criterion_group!(
    name    = book;
    config  = Criterion::default().sample_size(5_000);
    targets = bench_book_update
);
criterion_group!(
    name    = pipeline;
    config  = Criterion::default().sample_size(2_000);
    targets = bench_pipeline_single, bench_pipeline_batch
);
criterion_main!(ring, book, pipeline);
