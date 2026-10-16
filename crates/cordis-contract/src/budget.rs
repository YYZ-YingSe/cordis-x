//! Logical message limits, independent of JSON spelling and wire encoding.

pub const MAX_BYTES: usize = 1024 * 1024;
pub const MAX_NODES: usize = 65536;
pub const MAX_DEPTH: usize = 64;

/// A traversal owns its budget. Check before descending or subtracting bytes.
pub struct Budget {
    nodes: usize,
    bytes: usize,
}

impl Default for Budget {
    fn default() -> Self {
        Self {
            nodes: MAX_NODES,
            bytes: MAX_BYTES,
        }
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

    pub fn bytes(&mut self, count: usize) -> Result<(), String> {
        self.bytes = self
            .bytes
            .checked_sub(count)
            .ok_or("message byte limit exceeded")?;
        Ok(())
    }
}
