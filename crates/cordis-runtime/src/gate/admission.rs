//! The production admission protocol. Loom substitutes only its mutex.
use super::{Error, Phase, Result};
#[derive(Debug)]
pub(super) struct State {
    phase: Phase,
    leases: usize,
}
pub(super) trait Lock {
    fn with<T>(&self, f: impl FnOnce(&mut State) -> T) -> T;
}
impl Lock for std::sync::Mutex<State> {
    fn with<T>(&self, f: impl FnOnce(&mut State) -> T) -> T {
        f(&mut self.lock().unwrap())
    }
}
#[derive(Debug)]
pub(super) struct Admission<L = std::sync::Mutex<State>>(L);
impl Admission {
    pub fn new() -> Self {
        Self(std::sync::Mutex::new(State {
            phase: Phase::Loading,
            leases: 0,
        }))
    }
}
impl<L: Lock> Admission<L> {
    pub fn phase(&self) -> Phase {
        self.0.with(|s| s.phase)
    }
    pub fn count(&self) -> usize {
        self.0.with(|s| s.leases)
    }
    pub fn activate(&self) {
        self.0.with(|s| {
            assert_eq!(s.phase, Phase::Loading);
            s.phase = Phase::Active;
        });
    }
    pub fn close(&self) {
        self.0.with(|s| {
            if matches!(s.phase, Phase::Loading | Phase::Active) {
                s.phase = Phase::Closing;
            }
        });
    }
    pub fn recover(&self) {
        self.0.with(|s| {
            assert_eq!(s.phase, Phase::Closing);
            assert_eq!(s.leases, 0);
            s.phase = Phase::Recovering;
        });
    }
    pub fn finish(&self) {
        self.0.with(|s| s.phase = Phase::Closed);
    }
    pub fn enter(&self, loading: bool, continuation: bool, cleanup: bool) -> Result<()> {
        self.0.with(|s| {
            let allowed = if cleanup {
                s.phase != Phase::Closed
            } else {
                s.phase == Phase::Active
                    || (loading && s.phase == Phase::Loading)
                    || (continuation && s.phase == Phase::Closing)
            };
            if !allowed {
                return Err(Error::Closed);
            }
            s.leases += 1;
            Ok(())
        })
    }
    pub fn release(&self) {
        self.0.with(|s| {
            assert!(s.leases > 0);
            s.leases -= 1;
        });
    }
}
#[cfg(test)]
mod model {
    use super::*;
    use loom::sync::{Arc, Mutex};
    impl Lock for Mutex<State> {
        fn with<T>(&self, f: impl FnOnce(&mut State) -> T) -> T {
            f(&mut self.lock().unwrap())
        }
    }
    fn run(f: impl Fn() + Send + Sync + 'static) {
        let mut model = loom::model::Builder::new();
        model.preemption_bound = Some(2);
        model.max_branches = 10_000;
        model.check(f);
    }
    #[test]
    fn close_linearizes_with_admission_and_retirement() {
        run(|| {
            let gate = Arc::new(Admission(Mutex::new(State {
                phase: Phase::Active,
                leases: 0,
            })));
            let work = loom::thread::spawn({
                let gate = gate.clone();
                move || {
                    if gate.enter(false, false, false).is_ok() {
                        assert!(matches!(gate.phase(), Phase::Active | Phase::Closing));
                        loom::thread::yield_now();
                        gate.release();
                    }
                }
            });
            gate.close();
            assert!(gate.enter(false, false, false).is_err());
            // A zero observation after close cannot be followed by a newly
            // admitted ordinary call. recover asserts this under the same lock.
            if gate.count() == 0 {
                gate.recover();
                gate.finish();
            }
            work.join().unwrap();
            assert_eq!(gate.count(), 0);
            if gate.phase() == Phase::Closing {
                gate.recover();
                gate.finish();
            }
            assert!(gate.enter(true, true, true).is_err());
        });
    }
    #[test]
    fn late_continuation_keeps_the_parent_counted_until_completion() {
        run(|| {
            let gate = Arc::new(Admission(Mutex::new(State {
                phase: Phase::Loading,
                leases: 0,
            })));
            gate.enter(true, false, false).unwrap();
            let work = loom::thread::spawn({
                let gate = gate.clone();
                move || {
                    // The initializer/callback's admitted lease remains held while
                    // it starts a continuation, even if the owner begins closing.
                    gate.enter(true, true, false).unwrap();
                    gate.release();
                    loom::thread::yield_now();
                    gate.release();
                }
            });
            gate.close();
            assert!(gate.enter(true, false, false).is_err());
            work.join().unwrap();
            assert_eq!(gate.count(), 0);
            gate.recover();
            assert!(gate.enter(true, true, false).is_err());
            gate.enter(false, false, true).unwrap();
            gate.release();
            gate.finish();
            assert!(gate.enter(false, false, true).is_err());
        });
    }
}
