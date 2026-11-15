//! Bounded request identities. A slot can be used again only after its outgoing
//! request settles. The generation is never reused, including local failures.
//! Receivers retain one generation per slot, not every operation ever seen.
use std::sync::{Arc, Mutex};

pub const RESERVED_CAPACITY: usize = 16;
pub const REVERSE_LANES: [usize; 14] = [RESERVED_CAPACITY; 14];

#[derive(Clone, Copy)]
pub struct Layout {
    pub forward: [usize; 9],
    pub incoming: usize,
    pub output: usize,
    pub connections: usize,
}
impl Layout {
    pub fn new(ordinary: usize) -> Option<Self> {
        let incoming = ordinary.checked_add(8 * RESERVED_CAPACITY)?;
        let output = incoming.checked_add(REVERSE_LANES.iter().sum())?;
        let connections = ordinary.checked_add(RESERVED_CAPACITY)?.checked_mul(2)?;
        if ordinary == 0 || output > u32::MAX as usize || connections > u32::MAX as usize {
            return None;
        }
        let mut forward = [RESERVED_CAPACITY; 9];
        forward[0] = ordinary;
        Some(Self {
            forward,
            incoming,
            output,
            connections,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Id {
    pub slot: usize,
    pub generation: u64,
}
impl Id {
    pub fn parse(value: &str) -> Option<Self> {
        let (slot, generation) = value.split_once(':')?;
        let id = Self {
            slot: slot.parse().ok()?,
            generation: generation.parse().ok()?,
        };
        (id.generation != 0 && id.to_string() == value).then_some(id)
    }
}
impl std::fmt::Display for Id {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.slot, self.generation)
    }
}

#[derive(Clone, Copy, Default)]
struct Slot {
    generation: u64,
    active: bool,
}

/// Owned by the transport, not by a cancellable waiting future.
pub struct Outgoing {
    slots: Mutex<Vec<Slot>>,
    lanes: Vec<std::ops::Range<usize>>,
}
/// A reservation for executor-owned work. Do not attach it to a cancellable
/// public waiter: the work must keep it until its completion is observed.
pub struct Lease {
    owner: Arc<Outgoing>,
    pub id: Id,
}
impl Drop for Lease {
    fn drop(&mut self) {
        self.owner.complete(self.id);
    }
}
impl Outgoing {
    pub fn new(capacity: usize) -> Self {
        Self::with_lanes(&[capacity])
    }
    pub fn with_lanes(capacities: &[usize]) -> Self {
        let mut end = 0usize;
        let lanes = capacities
            .iter()
            .map(|capacity| {
                let start = end;
                end = end
                    .checked_add(*capacity)
                    .expect("operation capacity overflow");
                start..end
            })
            .collect();
        Self {
            slots: Mutex::new(vec![Slot::default(); end]),
            lanes,
        }
    }
    pub fn allocate(&self) -> Option<Id> {
        self.allocate_range(0..usize::MAX)
    }
    pub fn allocate_lane(&self, lane: usize) -> Option<Id> {
        self.allocate_range(self.lanes.get(lane)?.clone())
    }
    fn allocate_range(&self, range: std::ops::Range<usize>) -> Option<Id> {
        let mut slots = self.slots.lock().unwrap();
        let (slot, entry) = slots
            .iter_mut()
            .enumerate()
            .skip(range.start)
            .take(range.end.saturating_sub(range.start))
            .find(|(_, entry)| !entry.active && entry.generation != u64::MAX)?;
        entry.generation += 1;
        entry.active = true;
        Some(Id {
            slot,
            generation: entry.generation,
        })
    }
    pub fn acquire(self: &Arc<Self>) -> Option<Lease> {
        Some(Lease {
            owner: self.clone(),
            id: self.allocate()?,
        })
    }
    pub fn acquire_lane(self: &Arc<Self>, lane: usize) -> Option<Lease> {
        Some(Lease {
            owner: self.clone(),
            id: self.allocate_lane(lane)?,
        })
    }
    /// A stale completion cannot release the slot of a later request.
    pub fn complete(&self, id: Id) -> bool {
        let mut slots = self.slots.lock().unwrap();
        let Some(entry) = slots.get_mut(id.slot) else {
            return false;
        };
        if !entry.active || entry.generation != id.generation {
            return false;
        }
        entry.active = false;
        true
    }
}

#[derive(Clone)]
pub struct Incoming {
    slots: Arc<Mutex<Vec<Slot>>>,
}
pub struct Admission {
    slots: Arc<Mutex<Vec<Slot>>>,
    id: Id,
}
impl Incoming {
    pub fn new(capacity: usize) -> Self {
        Self {
            slots: Arc::new(Mutex::new(vec![Slot::default(); capacity])),
        }
    }
    pub fn grow(&self, capacity: usize) -> Result<(), String> {
        let mut slots = self.slots.lock().unwrap();
        let additional = capacity
            .checked_sub(slots.len())
            .ok_or("operation table cannot shrink")?;
        slots
            .try_reserve_exact(additional)
            .map_err(|error| error.to_string())?;
        slots.resize(capacity, Slot::default());
        Ok(())
    }
    pub fn admit(&self, id: Id) -> Option<Admission> {
        let mut slots = self.slots.lock().unwrap();
        let entry = slots.get_mut(id.slot)?;
        if entry.active || id.generation == 0 || id.generation <= entry.generation {
            return None;
        }
        *entry = Slot {
            generation: id.generation,
            active: true,
        };
        Some(Admission {
            slots: self.slots.clone(),
            id,
        })
    }
}
impl Drop for Admission {
    fn drop(&mut self) {
        let mut slots = self.slots.lock().unwrap();
        let entry = &mut slots[self.id.slot];
        debug_assert_eq!(entry.generation, self.id.generation);
        entry.active = false;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn full_business_lane_cannot_take_cleanup_identity_capacity() {
        let layout = Layout::new(257).unwrap();
        let sender = Outgoing::with_lanes(&layout.forward);
        let receiver = Incoming::new(1);
        let hello = sender.allocate_lane(0).unwrap();
        let admission = receiver.admit(hello).unwrap();
        receiver.grow(layout.incoming).unwrap();
        assert!(receiver.admit(hello).is_none());
        drop(admission);
        assert!(receiver.admit(hello).is_none());
        assert!(sender.complete(hello));
        let ordinary: Vec<_> = (0..257).map(|_| sender.allocate_lane(0).unwrap()).collect();
        assert!(sender.allocate_lane(0).is_none());
        let cleanup = sender.allocate_lane(1).unwrap();
        assert_eq!(cleanup.slot, 257);
        drop(receiver.admit(cleanup).unwrap());
        assert!(sender.complete(cleanup));
        for id in ordinary {
            assert!(sender.complete(id));
        }
    }
    #[test]
    fn independent_slots_keep_progressing_while_an_older_request_is_suspended() {
        let sender = Outgoing::new(2);
        let receiver = Incoming::new(2);
        let slow = sender.allocate().unwrap();
        let _slow = receiver.admit(slow).unwrap();
        for _ in 0..100_000 {
            let fast = sender.allocate().unwrap();
            let admitted = receiver.admit(fast).unwrap();
            assert!(receiver.admit(fast).is_none());
            assert!(sender.allocate().is_none());
            drop(admitted);
            assert!(sender.complete(fast));
            assert!(receiver.admit(fast).is_none());
        }
        assert!(!sender.complete(Id {
            slot: slow.slot,
            generation: slow.generation + 1
        }));
    }
    #[test]
    fn stale_completion_and_cross_transport_reordering_cannot_alias() {
        let sender = Outgoing::new(2);
        let receiver = Incoming::new(2);
        let a = sender.allocate().unwrap();
        let b = sender.allocate().unwrap();
        drop(receiver.admit(b).unwrap());
        drop(receiver.admit(a).unwrap());
        assert!(sender.complete(a));
        let c = sender.allocate().unwrap();
        assert_eq!(a.slot, c.slot);
        assert!(!sender.complete(a));
        let current = receiver.admit(c).unwrap();
        assert!(receiver.admit(a).is_none());
        assert!(
            receiver
                .admit(Id {
                    generation: c.generation + 1,
                    ..c
                })
                .is_none()
        );
        drop(current);
        assert!(
            receiver
                .admit(Id {
                    slot: 2,
                    generation: 1
                })
                .is_none()
        );
    }
    #[test]
    fn identities_are_canonical_and_generation_exhaustion_never_wraps() {
        for value in ["1", "01:1", "0:0", "0:01", "-1:1", "0:18446744073709551616"] {
            assert!(Id::parse(value).is_none());
        }
        assert_eq!(
            Id::parse("0:1"),
            Some(Id {
                slot: 0,
                generation: 1
            })
        );
        let sender = Outgoing::new(1);
        sender.slots.lock().unwrap()[0].generation = u64::MAX;
        assert!(sender.allocate().is_none());
    }
}
