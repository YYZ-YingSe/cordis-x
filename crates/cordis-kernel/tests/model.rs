use cordis_contract::{ContractId, RealmId, Slot};
use cordis_kernel::*;
use std::collections::{BTreeMap, VecDeque};

fn slot(node: usize) -> Slot {
    Slot::new(
        ContractId::new(format!("service-{node}"), "1"),
        RealmId::ROOT,
    )
}
fn spec(node: usize, deps: &[usize]) -> FiberSpec {
    let mut spec = FiberSpec::new(format!("fiber-{node}"));
    spec.provides.insert(slot(node));
    spec.requires.extend(deps.iter().map(|&i| slot(i)));
    spec
}
fn start(actions: &[Action]) -> Activation {
    actions
        .iter()
        .find_map(|a| match a {
            Action::Start { activation, .. } => Some(*activation),
            _ => None,
        })
        .unwrap()
}
fn publish(kernel: &mut Kernel, activation: Activation, node: usize) -> Vec<Action> {
    kernel
        .step(Event::Provided {
            activation,
            slot: slot(node),
        })
        .unwrap();
    kernel
        .step(Event::Initialized {
            activation,
            result: Ok(()),
        })
        .unwrap()
}

/// An ideal executor: complete every admitted action, with FIFO or LIFO scheduling.
/// No lifecycle reconciliation is duplicated here.
fn settle(
    kernel: &mut Kernel,
    actions: Vec<Action>,
    nodes: &BTreeMap<FiberId, usize>,
    reverse: bool,
) {
    let mut queue: VecDeque<_> = actions.into();
    let mut steps = 0;
    while let Some(action) = if reverse {
        queue.pop_back()
    } else {
        queue.pop_front()
    } {
        steps += 1;
        assert!(steps < 200, "non-converging lifecycle");
        let follow = match action {
            Action::Start { activation, .. } => {
                publish(kernel, activation, nodes[&activation.fiber])
            }
            Action::Withdraw(a) => kernel.step(Event::Drained(a)).unwrap(),
            Action::Recover(a) => kernel
                .step(Event::Recovered {
                    activation: a,
                    result: Ok(()),
                })
                .unwrap(),
            Action::Publish(_) | Action::Release(_) => Vec::new(),
        };
        kernel.check_invariants().unwrap();
        queue.extend(follow);
    }
}

/// Independent oracle: eliminate unavailable nodes until reaching a fixed point.
/// It contains no epochs, loading states, dirty index, or action queue.
fn reference(deps: &[Vec<usize>], installed: u8, retired: u8) -> u8 {
    let mut active = installed & !retired;
    loop {
        let next = deps
            .iter()
            .enumerate()
            .fold(active, |mask, (node, required)| {
                if required.iter().any(|&d| active & (1 << d) == 0) {
                    mask & !(1 << node)
                } else {
                    mask
                }
            });
        if next == active {
            return active;
        }
        active = next;
    }
}
fn compare(kernel: &Kernel, nodes: &BTreeMap<FiberId, usize>, expected: u8) {
    for (&id, &node) in nodes {
        let snapshot = kernel.snapshot(id).unwrap();
        let active = expected & (1 << node) != 0;
        assert_eq!(
            snapshot.state,
            if active {
                Lifecycle::Active
            } else {
                Lifecycle::Inactive
            }
        );
        assert_eq!(kernel.published(&slot(node)).is_some(), active);
        if !active {
            assert!(snapshot.committed.is_empty());
        }
    }
}
fn permutations() -> Vec<[usize; 4]> {
    let mut result = Vec::new();
    for a in 0..4 {
        for b in 0..4 {
            for c in 0..4 {
                for d in 0..4 {
                    if [a, b, c, d]
                        .into_iter()
                        .collect::<std::collections::BTreeSet<_>>()
                        .len()
                        == 4
                    {
                        result.push([a, b, c, d]);
                    }
                }
            }
        }
    }
    result
}

#[test]
fn all_four_node_forward_dags_match_independent_availability_model() {
    let orders = permutations();
    // 64 DAGs × 24 installation orders × 16 retirement sets × 2 schedules = 49,152 scenarios.
    for graph in 0u8..64 {
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
        for order in &orders {
            for retired in 0u8..16 {
                for reverse in [false, true] {
                    let mut kernel = Kernel::new(1);
                    let mut nodes = BTreeMap::new();
                    let mut installed = 0;
                    for &node in order {
                        let (id, actions) = kernel.install(spec(node, &deps[node])).unwrap();
                        nodes.insert(id, node);
                        installed |= 1 << node;
                        settle(&mut kernel, actions, &nodes, reverse);
                        compare(&kernel, &nodes, reference(&deps, installed, 0));
                    }
                    let mut actions = Vec::new();
                    for (&id, &node) in &nodes {
                        if retired & (1 << node) != 0 {
                            actions.extend(kernel.step(Event::Retire(id)).unwrap());
                        }
                    }
                    settle(&mut kernel, actions, &nodes, reverse);
                    compare(&kernel, &nodes, reference(&deps, installed, retired));
                }
            }
        }
    }
}

#[test]
fn staged_services_are_private_and_old_epochs_cannot_publish() {
    let mut kernel = Kernel::new(7);
    let (id, actions) = kernel.install(spec(0, &[])).unwrap();
    let first = start(&actions);
    kernel
        .step(Event::Provided {
            activation: first,
            slot: slot(0),
        })
        .unwrap();
    assert_eq!(kernel.published(&slot(0)), None);
    let (_, waiting) = kernel.install(spec(1, &[0])).unwrap();
    assert!(waiting.is_empty());
    kernel.step(Event::Restart(id)).unwrap();
    assert!(
        kernel.step(Event::Drained(first)).unwrap().is_empty(),
        "initializer still holds recovery barrier"
    );
    let actions = kernel
        .step(Event::Initialized {
            activation: first,
            result: Ok(()),
        })
        .unwrap();
    assert_eq!(actions, vec![Action::Recover(first)]);
    let actions = kernel
        .step(Event::Recovered {
            activation: first,
            result: Ok(()),
        })
        .unwrap();
    let second = start(&actions);
    assert!(second.epoch > first.epoch);
    assert_eq!(kernel.published(&slot(0)), None);
    assert_eq!(
        kernel.step(Event::Provided {
            activation: first,
            slot: slot(0)
        }),
        Err(Error::StaleActivation(first))
    );
    let alien = Activation {
        incarnation: 8,
        ..second
    };
    assert_eq!(
        kernel.step(Event::Drained(alien)),
        Err(Error::StaleActivation(alien))
    );
    publish(&mut kernel, second, 0);
    assert_eq!(kernel.published(&slot(0)), Some(second));
}

#[test]
fn failed_installation_is_transactional_and_missing_dependencies_can_form_cycles() {
    let mut kernel = Kernel::new(1);
    let (first, _) = kernel.install(spec(0, &[1])).unwrap();
    let before = format!("{kernel:?}");
    assert_eq!(
        kernel.install(spec(0, &[])).unwrap_err(),
        Error::SlotOccupied(slot(0))
    );
    assert_eq!(format!("{kernel:?}"), before);
    assert_eq!(
        kernel.install(spec(1, &[0])).unwrap_err(),
        Error::DependencyCycle
    );
    assert_eq!(format!("{kernel:?}"), before);
    let (second, _) = kernel.install(spec(1, &[])).unwrap();
    assert_eq!(second.0, first.0 + 1);
    kernel.check_invariants().unwrap();
}

#[test]
fn provider_recovery_waits_for_entire_consumer_and_its_own_drain() {
    let mut kernel = Kernel::new(1);
    let (provider, actions) = kernel.install(spec(0, &[])).unwrap();
    let p = start(&actions);
    publish(&mut kernel, p, 0);
    let (consumer, actions) = kernel.install(spec(1, &[0])).unwrap();
    let c = start(&actions);
    publish(&mut kernel, c, 1);
    let committed = kernel.snapshot(consumer).unwrap().committed;
    kernel.step(Event::Retire(provider)).unwrap();
    assert_eq!(kernel.snapshot(consumer).unwrap().committed, committed);
    assert!(kernel.snapshot(consumer).unwrap().target.is_none());
    assert_eq!(
        kernel.step(Event::Drained(c)).unwrap(),
        vec![Action::Recover(c)]
    );
    assert_eq!(
        kernel
            .step(Event::Recovered {
                activation: c,
                result: Ok(())
            })
            .unwrap(),
        vec![Action::Release(c)]
    );
    assert_eq!(
        kernel.step(Event::Drained(p)).unwrap(),
        vec![Action::Recover(p)]
    );
    assert_eq!(
        kernel
            .step(Event::Recovered {
                activation: p,
                result: Ok(())
            })
            .unwrap(),
        vec![Action::Release(p)]
    );
    // Only completed retirement frees the exclusive slot.
    assert!(kernel.install(spec(0, &[])).is_ok());
}

#[test]
fn cleanup_failure_is_residual_and_duplicate_completion_cannot_erase_it() {
    let mut kernel = Kernel::new(1);
    let (id, actions) = kernel.install(spec(0, &[])).unwrap();
    let a = start(&actions);
    publish(&mut kernel, a, 0);
    kernel.step(Event::Retire(id)).unwrap();
    kernel.step(Event::Drained(a)).unwrap();
    kernel
        .step(Event::Recovered {
            activation: a,
            result: Err("partial inverse".into()),
        })
        .unwrap();
    let before = format!("{kernel:?}");
    assert_eq!(
        kernel.step(Event::Recovered {
            activation: a,
            result: Ok(())
        }),
        Err(Error::UnexpectedCompletion(a))
    );
    assert_eq!(format!("{kernel:?}"), before);
    assert_eq!(kernel.snapshot(id).unwrap().state, Lifecycle::Unloading);
    assert_eq!(
        kernel.install(spec(0, &[])).unwrap_err(),
        Error::SlotOccupied(slot(0))
    );
}

#[test]
fn execution_fault_withdraws_consumers_and_requires_explicit_restart() {
    let mut kernel = Kernel::new(1);
    let (provider, actions) = kernel.install(spec(0, &[])).unwrap();
    let p = start(&actions);
    publish(&mut kernel, p, 0);
    let (consumer, actions) = kernel.install(spec(1, &[0])).unwrap();
    let c = start(&actions);
    publish(&mut kernel, c, 1);
    let nodes = BTreeMap::from([(provider, 0), (consumer, 1)]);
    let actions = kernel
        .step(Event::Fault {
            activation: p,
            message: "backend lost".into(),
        })
        .unwrap();
    assert!(kernel.published(&slot(0)).is_none());
    settle(&mut kernel, actions, &nodes, true);
    assert_eq!(
        kernel.snapshot(provider).unwrap().state,
        Lifecycle::Inactive
    );
    assert_eq!(
        kernel.snapshot(consumer).unwrap().state,
        Lifecycle::Inactive
    );
    assert!(matches!(
        kernel.snapshot(provider).unwrap().outcome,
        Some(Outcome::ExecutionFailed(_))
    ));
    let actions = kernel.step(Event::Restart(provider)).unwrap();
    settle(&mut kernel, actions, &nodes, false);
    assert_eq!(kernel.snapshot(consumer).unwrap().state, Lifecycle::Active);
    assert_eq!(
        kernel.step(Event::Fault {
            activation: p,
            message: "late fault".into()
        }),
        Err(Error::StaleActivation(p))
    );
}

#[test]
fn initialization_retirement_and_drain_permutations_match_barrier_oracle() {
    // Two independent precedence constraints: staging precedes initializer
    // completion; retirement precedes drain. Six legal schedules, each with
    // successful/failed initialization and successful/failed recovery.
    let mut scenarios = 0;
    for order in permutations() {
        let position = |event| order.iter().position(|v| *v == event).unwrap();
        if position(0) > position(1) || position(2) > position(3) {
            continue;
        }
        for initialize_ok in [false, true] {
            for recover_ok in [false, true] {
                scenarios += 1;
                let mut kernel = Kernel::new(1);
                let (id, actions) = kernel.install(spec(0, &[])).unwrap();
                let activation = start(&actions);
                let (mut initialized, mut retired, mut drained, mut recovery_started) =
                    (false, false, false, false);
                for event in order {
                    let event = match event {
                        0 => Event::Provided {
                            activation,
                            slot: slot(0),
                        },
                        1 => {
                            initialized = true;
                            Event::Initialized {
                                activation,
                                result: if initialize_ok {
                                    Ok(())
                                } else {
                                    Err("failed".into())
                                },
                            }
                        }
                        2 => {
                            retired = true;
                            Event::Retire(id)
                        }
                        3 => {
                            drained = true;
                            Event::Drained(activation)
                        }
                        _ => unreachable!(),
                    };
                    let actions = kernel.step(event).unwrap();
                    assert_eq!(
                        kernel.published(&slot(0)).is_some(),
                        initialized && initialize_ok && !retired
                    );
                    let ready = initialized && retired && drained;
                    assert_eq!(
                        actions.contains(&Action::Recover(activation)),
                        ready && !recovery_started
                    );
                    recovery_started |= ready;
                    kernel.check_invariants().unwrap();
                }
                let actions = kernel
                    .step(Event::Recovered {
                        activation,
                        result: if recover_ok {
                            Ok(())
                        } else {
                            Err("inverse failed".into())
                        },
                    })
                    .unwrap();
                assert_eq!(actions.contains(&Action::Release(activation)), recover_ok);
                assert_eq!(
                    kernel.snapshot(id).unwrap().state,
                    if recover_ok {
                        Lifecycle::Inactive
                    } else {
                        Lifecycle::Unloading
                    }
                );
                assert!(kernel.snapshot(id).unwrap().retired);
                assert!(kernel.published(&slot(0)).is_none());
                kernel.check_invariants().unwrap();
            }
        }
    }
    assert_eq!(scenarios, 24);
}
