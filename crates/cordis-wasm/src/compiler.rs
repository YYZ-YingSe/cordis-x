//! Host-owned, bounded cache of immutable Wasmtime code. Never caches a Store.
use crate::{Capabilities, Component, ContextComponent, Error, Limits, Result};
use sha2::{Digest, Sha256};
use std::{
    collections::VecDeque,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use wasmtime::{Config, Engine};

/// Bounds on compiled code retained by this cache. Live components own
/// their code independently, so these are not bounds on total process RSS.
#[derive(Clone, Copy, Debug)]
pub struct CacheLimits {
    pub entries: usize,
    /// Wasmtime's complete code image, including its read-only data. Engine and
    /// Rust metadata overhead is bounded by entry count, not included here.
    pub image_bytes: usize,
}
impl Default for CacheLimits {
    fn default() -> Self {
        Self {
            entries: 8,
            image_bytes: 64 * 1024 * 1024,
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct CompilerStats {
    pub hits: u64,
    pub misses: u64,
    pub compilations: u64,
    pub compile_time: Duration,
    pub preflights: u64,
    pub preflight_time: Duration,
    pub resident_entries: usize,
    pub resident_image_bytes: usize,
}

/// Each variant is a complete, fixed Wasmtime compilation policy, defined below.
/// Runtime limits and permissions belong to fresh Stores, never to cache keys.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum Profile {
    Basic,
    Context,
}
impl Profile {
    fn config(self) -> Config {
        let mut config = Config::new();
        config
            .wasm_component_model(true)
            .consume_fuel(true)
            .max_wasm_stack(512 * 1024);
        if self == Self::Context {
            config.wasm_exceptions(true).concurrency_support(true);
        }
        config
    }
}
pub(crate) struct CompiledCode {
    pub engine: Engine,
    pub component: wasmtime::component::Component,
}
struct Entry {
    profile: Profile,
    digest: [u8; 32],
    code: Arc<CompiledCode>,
    image_bytes: usize,
}
#[derive(Default)]
struct Cache {
    // Least recently used at the front; small and explicitly bounded.
    entries: VecDeque<Entry>,
    stats: CompilerStats,
}
struct Inner {
    limits: CacheLimits,
    cache: Mutex<Cache>,
}

/// Share one Compiler across Loader sources to share a single code budget.
/// The lock serializes cold compilation and bounds concurrent compiler work.
/// Waiting requests reuse the result while it remains resident; evicted or
/// non-retained code may be compiled again.
/// Store construction and metadata preflight happen outside the cache lock.
#[derive(Clone)]
pub struct Compiler(Arc<Inner>);
impl Default for Compiler {
    fn default() -> Self {
        Self::new(CacheLimits::default())
    }
}
impl Compiler {
    /// Either bound set to zero disables retention (compilation still works).
    pub fn new(limits: CacheLimits) -> Self {
        Self(Arc::new(Inner {
            limits,
            cache: Mutex::new(Cache::default()),
        }))
    }
    pub fn component(&self, bytes: &[u8], limits: Limits) -> Result<Component> {
        crate::engine::Module::compile(self, bytes, limits)
            .map(|module| Component(Arc::new(module)))
    }
    pub fn context_component(
        &self,
        bytes: &[u8],
        limits: Limits,
        capabilities: Capabilities,
    ) -> Result<ContextComponent> {
        ContextComponent::prepare(self, bytes, limits, capabilities)
    }
    pub fn stats(&self) -> CompilerStats {
        self.0.cache.lock().unwrap().stats.clone()
    }
    /// Drop cache references, without revoking live instances or their cleanup.
    /// An in-progress compilation finishes before this acquires the cache lock.
    pub fn clear(&self) {
        let mut cache = self.0.cache.lock().unwrap();
        cache.entries.clear();
        cache.stats.resident_entries = 0;
        cache.stats.resident_image_bytes = 0;
    }
    pub(crate) fn compile(&self, profile: Profile, bytes: &[u8]) -> Result<Arc<CompiledCode>> {
        let digest = Sha256::digest(bytes).into();
        let mut cache = self.0.cache.lock().unwrap();
        if let Some(index) = cache
            .entries
            .iter()
            .position(|e| e.profile == profile && e.digest == digest)
        {
            let entry = cache.entries.remove(index).unwrap();
            let code = entry.code.clone();
            cache.entries.push_back(entry);
            cache.stats.hits += 1;
            return Ok(code);
        }
        cache.stats.misses += 1;
        let start = Instant::now();
        let compiled: Result<Arc<CompiledCode>> = (|| {
            let engine = Engine::new(&profile.config()).map_err(|e| Error::Load(e.to_string()))?;
            // Source validation is mandatory. No native-code deserialization.
            let component = wasmtime::component::Component::from_binary(&engine, bytes)
                .map_err(|e| Error::Load(format!("{e:#}")))?;
            Ok(Arc::new(CompiledCode { engine, component }))
        })();
        cache.stats.compile_time += start.elapsed();
        let code: Arc<CompiledCode> = compiled?;
        cache.stats.compilations += 1;
        let image = code.component.image_range();
        let image_bytes = (image.end as usize).saturating_sub(image.start as usize);
        let limits = self.0.limits;
        if limits.entries > 0 && limits.image_bytes > 0 && image_bytes <= limits.image_bytes {
            while cache.entries.len() >= limits.entries
                || cache.stats.resident_image_bytes > limits.image_bytes - image_bytes
            {
                let evicted = cache
                    .entries
                    .pop_front()
                    .expect("nonempty over-budget cache");
                cache.stats.resident_image_bytes -= evicted.image_bytes;
            }
            cache.entries.push_back(Entry {
                profile,
                digest,
                code: code.clone(),
                image_bytes,
            });
            cache.stats.resident_entries = cache.entries.len();
            cache.stats.resident_image_bytes += image_bytes;
        }
        Ok(code)
    }
    pub(crate) fn preflight<T>(&self, run: impl FnOnce() -> Result<T>) -> Result<T> {
        let start = Instant::now();
        let result = run();
        let elapsed = start.elapsed();
        let mut cache = self.0.cache.lock().unwrap();
        cache.stats.preflights += 1;
        cache.stats.preflight_time += elapsed;
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn bytes(n: i32) -> Vec<u8> {
        wat::parse_str(format!(
            "(component (core module (func (export \"f\") (result i32) i32.const {n})))"
        ))
        .unwrap()
    }

    #[test]
    fn concurrent_requests_share_code_but_profiles_and_contents_do_not() {
        let compiler = Compiler::default();
        let bytes = bytes(1);
        let codes = std::thread::scope(|scope| {
            let threads: Vec<_> = (0..8)
                .map(|_| scope.spawn(|| compiler.compile(Profile::Basic, &bytes).unwrap()))
                .collect();
            threads
                .into_iter()
                .map(|t| t.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert!(codes.iter().all(|c| Arc::ptr_eq(c, &codes[0])));
        assert_eq!(compiler.stats().compilations, 1);
        assert_eq!(compiler.stats().hits, 7);
        let context = compiler.compile(Profile::Context, &bytes).unwrap();
        assert!(!Arc::ptr_eq(&context, &codes[0]));
        assert_eq!(compiler.stats().compilations, 2);
    }

    #[test]
    fn lru_eviction_and_clear_release_only_cache_ownership() {
        let compiler = Compiler::new(CacheLimits {
            entries: 2,
            image_bytes: usize::MAX,
        });
        let a = compiler.compile(Profile::Basic, &bytes(1)).unwrap();
        let b = compiler.compile(Profile::Basic, &bytes(2)).unwrap();
        let weak_b = Arc::downgrade(&b);
        drop(b);
        assert!(Arc::ptr_eq(
            &a,
            &compiler.compile(Profile::Basic, &bytes(1)).unwrap()
        ));
        let c = compiler.compile(Profile::Basic, &bytes(3)).unwrap();
        assert!(weak_b.upgrade().is_none());
        assert_eq!(compiler.stats().resident_entries, 2);
        let weak_a = Arc::downgrade(&a);
        compiler.clear();
        assert_eq!(compiler.stats().resident_image_bytes, 0);
        assert!(weak_a.upgrade().is_some());
        drop(a);
        assert!(weak_a.upgrade().is_none());
        assert!(c.component.resources_required().is_some());
    }

    #[test]
    fn image_budget_oversize_and_failed_compilation_do_not_grow_cache() {
        let probe = Compiler::default();
        let code = probe.compile(Profile::Basic, &bytes(1)).unwrap();
        let size = probe.stats().resident_image_bytes;
        assert!(size > 0);
        let cache = Compiler::new(CacheLimits {
            entries: 8,
            image_bytes: size,
        });
        for n in 1..5 {
            cache.compile(Profile::Basic, &bytes(n)).unwrap();
            assert!(cache.stats().resident_image_bytes <= size);
        }
        let small = Compiler::new(CacheLimits {
            entries: 8,
            image_bytes: size - 1,
        });
        small.compile(Profile::Basic, &bytes(1)).unwrap();
        assert_eq!(small.stats().resident_entries, 0);
        for _ in 0..2 {
            assert!(small.compile(Profile::Basic, b"invalid").is_err());
        }
        assert_eq!(small.stats().misses, 3);
        assert_eq!(small.stats().compilations, 1);
        let disabled = Compiler::new(CacheLimits {
            entries: 0,
            image_bytes: usize::MAX,
        });
        disabled.compile(Profile::Basic, &bytes(1)).unwrap();
        assert_eq!(disabled.stats().resident_entries, 0);
        drop(code);
    }
}
