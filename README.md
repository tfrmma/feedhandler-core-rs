# feedhandler

Normalized L2 feedhandler core for Binance, Bybit, and Hyperliquid.

SPSC ring buffer (Disruptor node-sequence) between the I/O thread and a
dedicated book-engine thread. No heap allocation on the hot path.

## Layout

```
src/
  types.rs        — NormalizedTick, BookSide, OrderBook
  ring_buffer.rs  — SpscDisruptor (SPSC, lock-free)
  book_engine.rs  — BookEngine, LatencyHistogram
  timer.rs        — RDTSC clock + TSC calibration
benches/
  latency.rs      — criterion benchmarks (ring, book, pipeline)
```

## Key invariants

| Struct | Size | Notes |
|---|---|---|
| `NormalizedTick` | 64 B | one cache line, `repr(C, align(64))` |
| `RingBufferNode` | 128 B | sequence on CL0, tick on CL1 |
| `BookSide` | 1088 B | count isolated on its own cache line |
| `OrderBook` | 2240 B | bids / asks / metadata on separate regions |

Prices and quantities are fixed-point integers (scale 1e-8). No float arithmetic on the hot path — conversion happens in the exchange adapter before the tick enters the ring.

## Snapshot protocol

A snapshot from the wire is N price levels. The adapter stamps every
`NormalizedTick` in the batch with `is_snapshot=true` and the **same**
non-zero `snapshot_id`. `OrderBook::apply` clears the relevant side once
(on the first tick whose id differs from the stored epoch) and inserts the
rest normally. No extra allocation, no separate batch type.

## Running benchmarks

Requires rustc ≥ 1.80 (criterion 0.5 dependency). Enable in `Cargo.toml`:

```toml
[dev-dependencies]
criterion = { version = "0.5", features = ["html_reports"] }

[[bench]]
name    = "latency"
harness = false
```

```sh
cargo bench --bench latency
```

For flamegraphs:

```sh
cargo flamegraph --bench latency -- --bench
```

## Production notes

- Pin the book-engine thread to an isolated core: `taskset -c 3 ./server`
- Call `timer::calibrate()` once at startup before spawning workers
- `RING_SIZE` (default 4096) trades latency for back-pressure tolerance;
  increase if your adapter bursts faster than the engine drains
- `MAX_LEVELS` (default 64) covers standard exchange depth snapshots;
  reduce for instruments where you only care about top-of-book
