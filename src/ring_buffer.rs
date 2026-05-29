use crate::types::NormalizedTick;
use std::cell::UnsafeCell;
use std::sync::atomic::{AtomicU64, Ordering};

// 4096 × 128B = 512KB. Fits in L2 on anything built after 2015.
// If you're running this on a machine where it doesn't fit, you have other problems.
pub const RING_SIZE: usize = 1 << 12;
const RING_MASK: u64 = (RING_SIZE - 1) as u64;

const _: () = assert!(RING_SIZE.is_power_of_two());

// Classic Disruptor node-sequence trick. Each slot owns its handshake counter,
// so producer and consumer coordinate without ever touching the same cache line.
//
// slot lifecycle:
//   seq == cursor           → writable  (starts at slot index)
//   seq == cursor + 1       → readable  (producer just finished)
//   seq == cursor + RING_SIZE → writable again (consumer released)
//
// sequence on CL0, tick payload on CL1. They never share a line.
#[repr(C, align(128))]
pub struct RingBufferNode {
    sequence: AtomicU64,
    _seq_pad: [u8; 56],
    tick:     UnsafeCell<NormalizedTick>,
}

// SAFETY: SPSC contract. One producer, one consumer, never the same slot at once.
// If you call try_publish from two threads I will find you.
unsafe impl Sync for RingBufferNode {}
unsafe impl Send for RingBufferNode {}

const _: () = assert!(std::mem::size_of::<RingBufferNode>() == 128);

// Separate cache lines for producer and consumer cursors.
// Without this padding you get false-sharing and lose most of your gains.
#[repr(C, align(64))]
struct Cursor {
    seq:  AtomicU64,
    _pad: [u8; 56],
}

impl Cursor {
    const fn new(v: u64) -> Self {
        Cursor { seq: AtomicU64::new(v), _pad: [0u8; 56] }
    }
}

// TODO: add an MPSC variant for the multi-feed case (Binance + Bybit + HL
// all writing into one book engine). For now callers just spin up one ring
// per exchange and let the engine drain them in priority order. Good enough.
pub struct SpscDisruptor {
    buffer:   Box<[RingBufferNode]>,
    producer: Cursor,
    consumer: Cursor,
}

// SAFETY: heap address is stable after construction. Node-sequence protocol
// ensures no concurrent access to the same slot.
unsafe impl Sync for SpscDisruptor {}
unsafe impl Send for SpscDisruptor {}

impl SpscDisruptor {
    pub fn new() -> Self {
        // Allocating here is fine, this is cold path. The hot path never allocates.
        let mut buf: Vec<RingBufferNode> = Vec::with_capacity(RING_SIZE);
        for i in 0..RING_SIZE {
            buf.push(RingBufferNode {
                sequence: AtomicU64::new(i as u64),
                _seq_pad: [0u8; 56],
                tick:     UnsafeCell::new(NormalizedTick::default()),
            });
        }
        SpscDisruptor {
            buffer:   buf.into_boxed_slice(),
            producer: Cursor::new(0),
            consumer: Cursor::new(0),
        }
    }

    /// Returns false if the ring is full. Caller decides whether to spin, drop, or panic.
    /// In production we spin for a few hundred ns then log and drop.
    ///
    /// # Safety  Single-producer only.
    #[inline(always)]
    pub unsafe fn try_publish(&self, tick: NormalizedTick) -> bool {
        let prod = self.producer.seq.load(Ordering::Relaxed);
        let node = self.buffer.get_unchecked((prod & RING_MASK) as usize);

        // Acquire pairs with the consumer's Release when it freed this slot.
        if node.sequence.load(Ordering::Acquire) != prod {
            return false;
        }

        node.tick.get().write(tick);
        // Release makes the tick visible before the consumer can read seq == prod+1.
        node.sequence.store(prod.wrapping_add(1), Ordering::Release);
        self.producer.seq.store(prod.wrapping_add(1), Ordering::Relaxed);
        true
    }

    /// # Safety  Single-consumer only.
    #[inline(always)]
    pub unsafe fn try_consume(&self) -> Option<NormalizedTick> {
        let cons = self.consumer.seq.load(Ordering::Relaxed);
        let node = self.buffer.get_unchecked((cons & RING_MASK) as usize);

        // Acquire synchronises with the producer's Release — tick is fully visible now.
        if node.sequence.load(Ordering::Acquire) != cons.wrapping_add(1) {
            return None;
        }

        let tick = node.tick.get().read();
        node.sequence.store(cons.wrapping_add(RING_SIZE as u64), Ordering::Release);
        self.consumer.seq.store(cons.wrapping_add(1), Ordering::Relaxed);
        Some(tick)
    }

    // Approximate. Don't use this for anything load-bearing.
    #[inline]
    pub fn pending(&self) -> u64 {
        self.producer.seq.load(Ordering::Relaxed)
            .wrapping_sub(self.consumer.seq.load(Ordering::Relaxed))
    }

    #[inline(always)]
    pub const fn capacity() -> usize { RING_SIZE }
}

impl Default for SpscDisruptor {
    fn default() -> Self { Self::new() }
}
