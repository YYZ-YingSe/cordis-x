use cordis_contract::{ContractId, RealmId, Slot};
use cordis_kernel::*;
use std::collections::{BTreeMap, BTreeSet, VecDeque};

fn slot(key: usize, realm: u64) -> Slot {
    Slot::new(ContractId::new(format!("key-{key}"), "1"), RealmId(realm))
}
fn spec(key: usize, realm: u64, deps: &[usize]) -> FiberSpec {
    let mut result = FiberSpec::new(format!("node-{key}"));
    result.requires = deps.iter().map(|key| slot(*key, realm)).collect();
    result.provides.insert(slot(key, realm));
    result
}
fn settle(kernel: &mut Kernel, actions: Vec<Action>, reverse: bool) {
    let mut queue: VecDeque<_> = actions.into();
    let mut steps = 0;
    while let Some(action) = if reverse {
        queue.pop_back()
    } else {
        queue.pop_front()
    } {
        steps += 1;
        assert!(steps < 200);
        let actions = match action {
            Action::Start { activation, .. } => {
                for slot in kernel.slots(activation.fiber).unwrap().provides {
                    kernel.step(Event::Provided { activation, slot }).unwrap();
                }
                kernel
                    .step(Event::Initialized {
                        activation,
                        result: Ok(()),
                    })
                    .unwrap()
            }
            Action::Withdraw(activation) => kernel.step(Event::Drained(activation)).unwrap(),
            Action::Recover(activation) => kernel
                .step(Event::Recovered {
                    activation,
                    result: Ok(()),
                })
                .unwrap(),
            _ => vec![],
        };
        kernel.check_invariants().unwrap();
        queue.extend(actions);
    }
}

#[test]
fn realm_partitions_match_independent_dag_oracle_and_round_trip() {
    // 64 DAGs × 16 realm partitions × 2 executor orders. No lifecycle algorithm
    // in the oracle: a node is available iff all its predecessors are available
    // in the same namespace. Then return every node to the original realm.
    for graph in 0..64 {
        let mut deps = vec![Vec::new(); 4];
        let mut bit = 0;
        for (consumer, required) in deps.iter_mut().enumerate() {
            for provider in 0..consumer {
                if graph & (1 << bit) != 0 {
                    required.push(provider);
                }
                bit += 1;
            }
        }
        for partition in 0..16 {
            for reverse in [false, true] {
                let mut kernel = Kernel::new(1);
                let mut ids = Vec::new();
                for (node, required) in deps.iter().enumerate() {
                    let (id, actions) = kernel.install(spec(node, 0, required)).unwrap();
                    ids.push(id);
                    settle(&mut kernel, actions, reverse);
                }
                let initial = kernel.snapshots();
                let mut expected = [false; 4];
                for node in 0..4 {
                    expected[node] = deps[node].iter().all(|provider| {
                        expected[*provider]
                            && ((partition >> node) & 1) == ((partition >> provider) & 1)
                    });
                }
                let changes = ids
                    .iter()
                    .enumerate()
                    .map(|(node, id)| {
                        let spec = spec(node, ((partition >> node) & 1) as u64, &deps[node]);
                        (
                            *id,
                            RealmSlots {
                                requires: spec.requires,
                                provides: spec.provides,
                            },
                        )
                    })
                    .collect();
                let actions = kernel.reconfigure_realms(&changes).unwrap();
                settle(&mut kernel, actions, reverse);
                for node in 0..4 {
                    let snapshot = kernel.snapshot(ids[node]).unwrap();
                    assert_eq!(snapshot.state == Lifecycle::Active, expected[node]);
                    if expected[node] {
                        assert_eq!(snapshot.activation, initial[node].activation);
                        assert_eq!(snapshot.committed, initial[node].committed);
                    }
                }
                let changes = ids
                    .iter()
                    .enumerate()
                    .map(|(node, id)| {
                        let spec = spec(node, 0, &deps[node]);
                        (
                            *id,
                            RealmSlots {
                                requires: spec.requires,
                                provides: spec.provides,
                            },
                        )
                    })
                    .collect();
                let actions = kernel.reconfigure_realms(&changes).unwrap();
                settle(&mut kernel, actions, reverse);
                assert!(
                    kernel
                        .snapshots()
                        .iter()
                        .all(|s| s.state == Lifecycle::Active)
                );
            }
        }
    }
}

#[test]
fn collisions_and_cycles_leave_all_kernel_state_unchanged() {
    let mut kernel = Kernel::new(1);
    let (a, actions) = kernel.install(spec(0, 0, &[])).unwrap();
    settle(&mut kernel, actions, false);
    let (b, actions) = kernel.install(spec(0, 1, &[])).unwrap();
    settle(&mut kernel, actions, false);
    let before = format!("{kernel:?}");
    assert_eq!(
        kernel.reconfigure_realms(&BTreeMap::from([(a, kernel.slots(b).unwrap())])),
        Err(Error::SlotOccupied(slot(0, 1)))
    );
    assert_eq!(format!("{kernel:?}"), before);

    let mut kernel = Kernel::new(1);
    let (a, _) = kernel.install(spec(0, 0, &[1])).unwrap();
    let (b, _) = kernel.install(spec(1, 1, &[0])).unwrap();
    let before = format!("{kernel:?}");
    let change = spec(1, 0, &[0]);
    assert_eq!(
        kernel.reconfigure_realms(&BTreeMap::from([(
            b,
            RealmSlots {
                requires: change.requires,
                provides: change.provides
            }
        )])),
        Err(Error::DependencyCycle)
    );
    assert_eq!(format!("{kernel:?}"), before);
    assert_eq!(kernel.snapshot(a).unwrap().state, Lifecycle::Inactive);
}

#[test]
fn unfinished_activation_lands_in_original_namespace_and_holds_both_reservations() {
    let mut kernel = Kernel::new(1);
    let (id, actions) = kernel.install(spec(0, 0, &[])).unwrap();
    let first = match actions[0] {
        Action::Start { activation, .. } => activation,
        _ => unreachable!(),
    };
    let change = RealmSlots {
        requires: BTreeSet::new(),
        provides: BTreeSet::from([slot(0, 1)]),
    };
    assert_eq!(
        kernel
            .reconfigure_realms(&BTreeMap::from([(id, change)]))
            .unwrap(),
        vec![Action::Withdraw(first)]
    );
    assert!(kernel.step(Event::Drained(first)).unwrap().is_empty());
    for realm in [0, 1] {
        assert!(matches!(
            kernel.validate_replacement(&BTreeSet::new(), &[spec(0, realm, &[])]),
            Err(Error::SlotOccupied(_))
        ));
        assert!(matches!(
            kernel.install(spec(0, realm, &[])),
            Err(Error::SlotOccupied(_))
        ));
    }
    kernel
        .step(Event::Provided {
            activation: first,
            slot: slot(0, 0),
        })
        .unwrap();
    assert_eq!(
        kernel
            .step(Event::Initialized {
                activation: first,
                result: Ok(())
            })
            .unwrap(),
        vec![Action::Recover(first)]
    );
    let actions = kernel
        .step(Event::Recovered {
            activation: first,
            result: Ok(()),
        })
        .unwrap();
    settle(&mut kernel, actions, false);
    assert!(kernel.published(&slot(0, 0)).is_none());
    assert_eq!(
        kernel.published(&slot(0, 1)),
        kernel.snapshot(id).unwrap().activation
    );
    assert!(kernel.install(spec(0, 0, &[])).is_ok());
}
