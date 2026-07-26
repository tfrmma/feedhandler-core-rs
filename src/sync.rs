// Swap between std and loom's tracked types depending on --cfg loom. Nothing
// outside this file should import std::sync::atomic or std::cell::UnsafeCell
// directly, that's how the loom build stays honest about what it's checking.
//
// Run the model checker with:
//   RUSTFLAGS="--cfg loom" cargo test --release --test loom_ring_buffer

#[cfg(loom)]
pub use loom::sync::atomic::{AtomicU64, Ordering};
#[cfg(not(loom))]
pub use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(loom)]
pub use loom::cell::UnsafeCell;

#[cfg(not(loom))]
#[derive(Debug)]
pub struct UnsafeCell<T>(std::cell::UnsafeCell<T>);

#[cfg(not(loom))]
impl<T> UnsafeCell<T> {
    pub fn new(data: T) -> Self {
        Self(std::cell::UnsafeCell::new(data))
    }

    #[inline(always)]
    pub fn with<R>(&self, f: impl FnOnce(*const T) -> R) -> R {
        f(self.0.get())
    }

    #[inline(always)]
    pub fn with_mut<R>(&self, f: impl FnOnce(*mut T) -> R) -> R {
        f(self.0.get())
    }
}
