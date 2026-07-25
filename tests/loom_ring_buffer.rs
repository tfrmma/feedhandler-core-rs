// Model-checks the producer/consumer handshake in SpscDisruptor. Loom
// re-runs the body under every legal thread interleaving and yells if it
// finds a data race, a lost update, or two threads aliasing the same
// UnsafeCell mutably. This is the thing the regular test suite can't catch,
// those tests all run on one thread and only prove the logic is right when
// nothing is racing.
//
// Doesn't run under a normal `cargo test`, needs the loom cfg flag:
//   RUSTFLAGS="--cfg loom" cargo test --release --test loom_ring_buffer
//
// Run in release, the model checker is slow enough without debug overhead
// on top. RING_SIZE drops to 4 under loom (see ring_buffer.rs) or this
// would never terminate.
#![cfg(loom)]

use feedhandler::ring_buffer::{SpscDisruptor, RING_SIZE};
use feedhandler::{Exchange, NormalizedTick, Price, Qty, Side, Symbol};
use loom::sync::Arc;
use loom::thread;

fn tick(seq: u64) -> NormalizedTick {
    NormalizedTick::new(
        Price::new(seq),
        Qty::new(1),
        seq,
        0,
        0,
        Symbol::from_bytes(b"BTCUSDT"),
        Exchange::Binance,
        Side::Bid,
        false,
        0,
    )
}

// Producer and consumer race for real, both spin until they succeed, same
// as any real caller would. If the sequences come out wrong or out of
// order, the handshake is broken.
#[test]
fn concurrent_publish_consume_preserves_order() {
    loom::model(|| {
        let ring = Arc::new(SpscDisruptor::new());
        const N: u64 = 3;

        let producer = {
            let ring = Arc::clone(&ring);
            thread::spawn(move || {
                for i in 0..N {
                    while !unsafe { ring.try_publish(tick(i)) } {
                        thread::yield_now();
                    }
                }
            })
        };

        let consumer = thread::spawn(move || {
            let mut seen = Vec::with_capacity(N as usize);
            while seen.len() < N as usize {
                match unsafe { ring.try_consume() } {
                    Some(t) => seen.push(t.sequence),
                    None => thread::yield_now(),
                }
            }
            seen
        });

        producer.join().unwrap();
        assert_eq!(consumer.join().unwrap(), (0..N).collect::<Vec<_>>());
    });
}

// Same thing but with more items than the ring has slots, so the producer
// has to actually block on a full ring and the wraparound path gets
// exercised, not just the fast path. Keep this small, loom's exhaustive
// search blows up fast; RING_SIZE+1 is the minimum that forces a wrap.
#[test]
fn wraps_around_without_dropping_or_reordering() {
    loom::model(|| {
        let ring = Arc::new(SpscDisruptor::new());
        let n = RING_SIZE as u64 + 1;

        let producer = {
            let ring = Arc::clone(&ring);
            thread::spawn(move || {
                for i in 0..n {
                    while !unsafe { ring.try_publish(tick(i)) } {
                        thread::yield_now();
                    }
                }
            })
        };

        let consumer = thread::spawn(move || {
            let mut last = None;
            for _ in 0..n {
                let t = loop {
                    if let Some(t) = unsafe { ring.try_consume() } {
                        break t;
                    }
                    thread::yield_now();
                };
                if let Some(prev) = last {
                    assert_eq!(t.sequence, prev + 1, "gap or reorder across the wrap");
                }
                last = Some(t.sequence);
            }
        });

        producer.join().unwrap();
        consumer.join().unwrap();
    });
}
