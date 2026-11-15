//! Structural traversal limits, independent of business payload size.

pub const MAX_NODES: usize = 65536;
pub const MAX_DEPTH: usize = 64;

/// A traversal owns its node budget and checks depth before descending.
pub struct Budget {
    nodes: usize,
}

impl Default for Budget {
    fn default() -> Self {
        Self { nodes: MAX_NODES }
    }
}

impl Budget {
    pub fn take(&mut self, depth: usize) -> Result<(), String> {
        if depth > MAX_DEPTH || self.nodes == 0 {
            return Err("message depth/node limit exceeded".into());
        }
        self.nodes -= 1;
        Ok(())
    }
}
