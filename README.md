# feedhandler

Normalized L2 order book core for Binance, Bybit, and Hyperliquid. Takes
exchange-specific deltas and snapshots, once your adapter has normalized them
into a single tick format, and maintains a correct, cache-friendly order book
from them at low and predictable latency.

This crate does one job: turn a stream of `NormalizedTick`s into a live
`OrderBook`. It does not speak any exchange's wire protocol, does not manage
WebSocket connections, and does not decide what to do when data looks wrong.
Those are adapter and strategy-layer concerns. What it guarantees is that
once a tick is handed to it, the book update is correct, allocation-free, and
bounded in latency, and that anything that looks like bad data (wrong
symbol, a sequence gap, a crossed book) is counted, not silently absorbed.

## Pipeline

```
exchange WS/adapter          this crate                        strategy layer
────────────────────    ─────────────────────────────    ──────────────────────
raw exchange frame  ──▶  NormalizedTick (64B, one          ──▶  OrderBook queries
                         cache line)                            (best bid/ask,
        │                        │                               spread, depth)
        │                        ▼
        │                SpscDisruptor (lock-free
        │                ring, one producer thread,
        └── I/O thread   one consumer thread)
                                 │
                                 ▼
                         BookEngine::run_batch()
                         on a pinned core, drains
                         the ring into OrderBook
```

One producer thread normalizes and pushes ticks; one consumer thread (ideally
pinned to its own core) drains the ring and applies them to the book. They
never share a cache line except by design, at the ring's handshake points.

## Layout

```
src/
  types.rs        OrderBook, BookSide, NormalizedTick, Price/Qty/Symbol
  ring_buffer.rs  SpscDisruptor, the lock-free SPSC ring
  book_engine.rs  BookEngine (drains the ring), LatencyHistogram
  timer.rs        RDTSC clock + TSC calibration
  sync.rs         std/loom shim, only relevant if you're running the
                  model checker (see Testing below)
tests/
  loom_ring_buffer.rs   loom-driven concurrency tests for the ring
benches/
  latency.rs      criterion benchmarks (ring, book, full pipeline)
```

## Key data structures

| Struct | Size | Notes |
|---|---|---|
| `NormalizedTick` | 64 B | one cache line, `repr(C, align(64))` |
| `RingBufferNode` | 128 B | sequence counter on CL0, tick payload on CL1, never share a line |
| `BookSide` | 1088 B | `count` isolated on its own cache line so updating it doesn't thrash the levels array |
| `OrderBook` | 2304 B | bids / asks / metadata each on separate cache-line-aligned regions |

Prices and quantities are fixed-point `u64` (1e-8 scale). No floats anywhere
on the hot path, conversion to whatever precision an exchange actually uses
happens in the adapter, before a tick ever enters the ring.

Every struct above has a `const _: () = assert!(size_of::<T>() == N)` next to
its definition. If you add a field and it doesn't fit the layout you expect,
that assert fails at compile time, not as a surprise in a profiler three
months later.

## Design decisions

**Fixed-size levels, no `Vec` in the book.** `BookSide` holds a flat
`[Level; 64]` array, insert and remove are `O(depth)` memmoves
(`ptr::copy`), not allocations. At realistic book depths this beats a
`BTreeMap` or a `Vec`-with-reallocation approach on every latency
percentile that matters, insert/remove cost is bounded by `MAX_LEVELS`, not
by heap behavior.

**SPSC ring, not a channel.** `std::sync::mpsc` and friends are built for
the general case and pay for it, allocation, dynamic dispatch, or at least a
lock or CAS loop on every send. The Disruptor node-sequence pattern here
gives each slot its own handshake counter, so the producer and consumer
coordinate without ever touching the same cache line on the fast path. If
you need multiple producers (e.g. Binance and Bybit both writing into one
book), run one ring per source and drain them from `BookEngine` in priority
order, this crate deliberately doesn't try to be MPSC.

**Snapshot batching without a separate batch type.** A wire snapshot is N
price levels. Rather than model that as its own type, the adapter just
stamps every `NormalizedTick` in the batch with `is_snapshot=true` and the
same `snapshot_id`. `OrderBook::apply` clears the relevant side exactly once
(on the first tick whose id differs from the stored one) and inserts the
rest as normal deltas. No extra allocation, no batch-assembly buffer.

**Sentinels, chosen carefully.** `bids_snapshot_id` / `asks_snapshot_id`
default to `u32::MAX`, not `0`. Adapters commonly start their own
`snapshot_id` counter at 0, and an earlier version of this defaulted to 0 as
well, so a real snapshot with `id=0` was indistinguishable from "no snapshot
applied yet" and silently failed to clear stale levels. Sequence tracking
learned the same lesson the other way: rather than pick another sentinel
value for "no tick applied yet," it uses an explicit `sequence_initialized`
flag, so a real first sequence number of 0 can never be miscounted as a gap.

**Counted, not rejected.** Nothing in `apply()` drops a tick over sequence
or ordering. If your feed reconnects and replays a few sequence numbers,
that's not this crate's call to make, it counts the anomaly and keeps
applying. The one thing that is rejected outright is a tick for the wrong
`Symbol`, since applying it would corrupt an unrelated book with no way to
undo it; see Health counters below for what happens then.

## Health counters

None of these change what `apply()` does to the book. They exist so a
monitoring loop can alert on conditions that would otherwise be invisible
until something downstream looked wrong.

| Field / method | On `OrderBook` unless noted | Meaning |
|---|---|---|
| `symbol_mismatches: u64` | | Tick's symbol didn't match the book's. Discarded, never applied. Debug builds also `debug_assert!` immediately, panic on the spot instead of waiting for a metrics dashboard, release builds count and move on. |
| `sequence_gaps: u64` | | `tick.sequence` jumped by more than 1, likely a dropped packet. |
| `sequence_reorders: u64` | | `tick.sequence` went backward or repeated, likely a reconnect replay or duplicate. |
| `checksum_failures: u64` | | Adapter-reported checksum mismatch (see below), you call `record_checksum_failure()`. |
| `is_stale(now_ns, threshold_ns) -> bool` | | No update within `threshold_ns` of `now_ns`. A book that's never received a tick is stale by construction, `last_update_ns` is still 0. |
| `is_crossed() -> bool` | | Best bid trades through best ask. `spread()` clamps this to `Some(0)` via `saturating_sub`, which reads identically to a tight, healthy market unless you check this separately. |
| `dropped_count()` | on `SpscDisruptor` | Ticks lost because the ring was full when `try_publish` was called. |

**Checksum validation is intentionally partial.** Bybit and OKX ship a
checksum with their L2 deltas specifically so consumers can detect desync,
but the concatenation format and CRC32 seed are exchange-specific and not
something this crate should guess at. `OrderBook::for_checksum(n, f)` walks
the top `n` levels per side, best price first, in the order the book already
stores them, so your adapter doesn't have to reach into `bids`/`asks`
directly to build whatever string or byte layout that exchange's checksum
expects. Once your adapter has computed and compared it, call
`record_checksum_failure()` to keep the result next to the book's other
health counters.

## Testing

Unit tests (`cargo test`) cover book mechanics: insert/remove/sort order,
snapshot batching, the sentinel and sequence-tracking regressions above, and
the ring's overflow/drop-counting behavior. These run single-threaded and
prove the logic is correct in isolation.

They don't prove the ring is correct *under concurrency*, so
`tests/loom_ring_buffer.rs` uses [loom](https://github.com/tokio-rs/loom) to
exhaustively model-check every legal interleaving of the producer and
consumer threads against the real `Acquire`/`Release` handshake:

```sh
RUSTFLAGS="--cfg loom" cargo test --release --test loom_ring_buffer
```

Loom needs its own tracked atomic and cell types instead of `std`'s, see
`src/sync.rs`, that's the only reason this crate has a `#[cfg(loom)]`
anywhere. It's not compiled or fetched as a dependency unless you pass that
flag. `RING_SIZE` also drops to 2 under `--cfg loom` (see `ring_buffer.rs`),
loom's exhaustive search is exponential in the state space, and a real
4096-slot ring would never finish checking.

## Running benchmarks

Requires rustc ≥ 1.80 (criterion 0.5 depends on `half` 2.x). Enable in
`Cargo.toml`:

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
- `RING_SIZE` (default 4096) trades memory and latency for back-pressure
  tolerance, increase it if your adapter can burst faster than the engine
  drains; watch `dropped_count()` in production to know if you need to
  - Every `try_publish` that returns `false` is already counted there, so a
    rising `dropped_count()` is your signal, not a guess
- `MAX_LEVELS` (default 64) covers standard exchange depth snapshots, reduce
  it for instruments where only top-of-book matters
- Wire `is_stale()`, `is_crossed()`, `sequence_gaps`, `sequence_reorders`,
  `symbol_mismatches`, and `checksum_failures` into whatever this crate
  doesn't provide, there's no metrics exporter here, they're just counters

## Not in scope, on purpose

This crate stays out of a few things deliberately, since bundling them in
would mean guessing at requirements it can't verify:

- **Exchange wire protocols and WebSocket handling.** Adapter layer, not
  here.
- **Multi-producer fan-in.** One `SpscDisruptor` per source; run one per
  exchange and drain them from `BookEngine` in whatever priority order your
  strategy needs.
- **Metrics export.** The health counters above are plain fields and
  methods, wire them into Prometheus, statsd, or whatever you're already
  running.
- **Capture/replay.** `NormalizedTick` is `repr(C)` and a fixed 64 bytes, so
  it's mmap-friendly if you want to build that on top, but this crate
  doesn't ship it.
