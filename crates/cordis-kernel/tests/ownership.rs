use cordis_kernel::*;
use std::collections::{BTreeMap, VecDeque};

fn settle(k: &mut Kernel, actions: Vec<Action>, reverse: bool) {
    let mut queue: VecDeque<_> = actions.into();
    while let Some(action) = if reverse {
        queue.pop_back()
    } else {
        queue.pop_front()
    } {
        let actions = match action {
            Action::Start { activation, .. } => k.step(Event::Initialized {
                activation,
                result: Ok(()),
            }),
            Action::Withdraw(a) => k.step(Event::Drained(a)),
            Action::Recover(activation) => k.step(Event::Recovered {
                activation,
                result: Ok(()),
            }),
            _ => Ok(vec![]),
        }
        .unwrap();
        k.check_invariants().unwrap();
        queue.extend(actions);
    }
}

#[test]
fn all_four_node_parent_maps_match_forest_and_retirement_oracle() {
    let mut base = Kernel::new(1);
    let mut ids = Vec::new();
    for i in 0..4 {
        let (id, actions) = base.install(FiberSpec::new(format!("node-{i}"))).unwrap();
        settle(&mut base, actions, false);
        ids.push(id);
    }
    let activations: Vec<_> = ids
        .iter()
        .map(|&id| base.snapshot(id).unwrap().activation.unwrap())
        .collect();
    let mut accepted = 0;
    for encoded in 0..625 {
        let mut digits = encoded;
        let parents: Vec<_> = (0..4)
            .map(|_| {
                let p = digits % 5;
                digits /= 5;
                (p != 4).then_some(p)
            })
            .collect();
        // Independent finite-function oracle: every path reaches None in at
        // most four edges iff this map is a forest. No kernel DFS/lifecycle code.
        let forest = (0..4).all(|node| {
            let mut cursor = Some(node);
            for _ in 0..4 {
                cursor = cursor.and_then(|i| parents[i]);
            }
            cursor.is_none()
        });
        let updates: BTreeMap<_, _> = (0..4)
            .map(|i| (ids[i], parents[i].map(|p| activations[p])))
            .collect();
        let mut moved = base.clone();
        let result = moved.reparent(&updates, &[]);
        if !forest {
            assert_eq!(result, Err(Error::OwnershipCycle));
            assert_eq!(format!("{moved:?}"), format!("{base:?}"));
            continue;
        }
        result.unwrap();
        accepted += 1;
        for i in 0..4 {
            assert_eq!(
                moved.snapshot(ids[i]).unwrap().activation,
                Some(activations[i])
            );
        }
        for root in 0..4 {
            for reverse in [false, true] {
                let mut k = moved.clone();
                let actions = k.step(Event::Retire(ids[root])).unwrap();
                settle(&mut k, actions, reverse);
                for (i, &id) in ids.iter().enumerate() {
                    let mut cursor = Some(i);
                    let mut stopped = false;
                    while let Some(node) = cursor {
                        stopped |= node == root;
                        cursor = parents[node];
                    }
                    let snapshot = k.snapshot(id).unwrap();
                    assert_eq!(snapshot.retired, stopped);
                    assert_eq!(snapshot.state == Lifecycle::Inactive, stopped);
                }
            }
        }
    }
    assert_eq!(accepted, 125); // 1,000 retirement schedules + 500 rejected maps.
}

#[test]
fn loading_child_moves_without_losing_setup_and_retirement_cannot_be_undone() {
    let mut k = Kernel::new(1);
    let (old, actions) = k.install(FiberSpec::new("old")).unwrap();
    settle(&mut k, actions, false);
    let (new, actions) = k.install(FiberSpec::new("new")).unwrap();
    settle(&mut k, actions, false);
    let mut spec = FiberSpec::new("loading");
    spec.parent = k.snapshot(old).unwrap().activation;
    let (child, start) = k.install(spec).unwrap();
    let a = k.snapshot(child).unwrap().activation;
    k.reparent(
        &BTreeMap::from([(child, k.snapshot(new).unwrap().activation)]),
        &[],
    )
    .unwrap();
    let actions = k.step(Event::Retire(old)).unwrap();
    settle(&mut k, actions, false);
    assert_eq!(k.snapshot(child).unwrap().state, Lifecycle::Loading);
    settle(&mut k, start, false);
    assert_eq!(k.snapshot(child).unwrap().activation, a);
    let actions = k.step(Event::Retire(new)).unwrap();
    let before = format!("{k:?}");
    assert_eq!(
        k.reparent(&BTreeMap::from([(child, None)]), &[]),
        Err(Error::Retired(child))
    );
    assert_eq!(format!("{k:?}"), before);
    settle(&mut k, actions, true);
    assert_eq!(k.snapshot(child).unwrap().state, Lifecycle::Inactive);
}
