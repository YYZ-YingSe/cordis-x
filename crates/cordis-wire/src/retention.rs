use std::sync::atomic::{AtomicUsize, Ordering};

/// Sweep expendable history under the owning registry's lock. Live entries
/// determine the next sweep interval, never whether an operation is admitted.
pub struct Sweep(AtomicUsize);

impl Default for Sweep {
    fn default() -> Self {
        Self(AtomicUsize::new(4096))
    }
}

impl Sweep {
    pub fn due(&self, size: usize) -> bool {
        size >= self.0.load(Ordering::Relaxed)
    }

    pub fn retained(&self, size: usize) {
        self.0
            .store(size.saturating_mul(2).max(4096), Ordering::Relaxed);
    }
}
