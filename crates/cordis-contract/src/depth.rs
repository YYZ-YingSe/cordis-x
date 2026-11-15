pub const MAX_DEPTH: usize = 64;

pub fn check_depth(depth: usize) -> Result<(), String> {
    if depth > MAX_DEPTH {
        return Err("message depth limit exceeded".into());
    }
    Ok(())
}
