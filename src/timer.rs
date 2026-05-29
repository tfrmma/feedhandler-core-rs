use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

static TSC_HZ: AtomicU64 = AtomicU64::new(0);

/// Sleep 100ms, count ticks, cache the ratio. Call once at startup.
/// Not robust against TSC freq scaling — disable turbo boost / C-states
/// on your trading box or this will drift under load.
pub fn calibrate() -> u64 {
    let t0 = Instant::now();
    let c0 = rdtsc();
    std::thread::sleep(Duration::from_millis(100));

    let elapsed_ns    = t0.elapsed().as_nanos() as u64;
    let elapsed_ticks = rdtsc().wrapping_sub(c0);
    let hz = elapsed_ticks
        .saturating_mul(1_000_000_000)
        .checked_div(elapsed_ns)
        .unwrap_or(3_000_000_000); // 3GHz fallback if something goes very wrong

    TSC_HZ.store(hz, Ordering::Relaxed);
    hz
}

#[inline(always)]
pub fn ticks_to_ns(ticks: u64) -> u64 {
    let hz = TSC_HZ.load(Ordering::Relaxed);
    if hz == 0 { return ticks / 3; } // calibrate() wasn't called, assume 3GHz, good enough
    ((ticks as u128).saturating_mul(1_000_000_000) / hz as u128) as u64
}

/// ~4ns, never syscalls. Use this everywhere on the hot path.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub fn rdtsc() -> u64 {
    unsafe { core::arch::x86_64::_rdtsc() }
}

/// Serialising variant — use at the *start* of a measured window to stop
/// the CPU from reordering instructions across the measurement boundary.
#[cfg(target_arch = "x86_64")]
#[inline(always)]
pub fn rdtscp() -> u64 {
    let mut _aux = 0u32;
    unsafe { core::arch::x86_64::__rdtscp(&mut _aux) }
}

#[cfg(not(target_arch = "x86_64"))]
#[inline(always)] pub fn rdtsc()  -> u64 { now_ns() }
#[cfg(not(target_arch = "x86_64"))]
#[inline(always)] pub fn rdtscp() -> u64 { now_ns() }

/// Syscall-backed wall clock. Fine for tests and cold paths, not the hot path.
#[inline(always)]
pub fn now_ns() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos() as u64
}
