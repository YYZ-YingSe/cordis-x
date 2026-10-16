use super::*;
use std::sync::atomic::AtomicUsize;

static RELEASED: AtomicUsize = AtomicUsize::new(0);
unsafe extern "C" fn release_host(_: *mut CordisAbiResult) {
    RELEASED.fetch_add(1, Ordering::SeqCst);
}
unsafe extern "C" fn invoke(
    _: u64,
    _: u64,
    _: CordisAbiSlice,
    _: CordisAbiSlice,
    _: *const CordisAbiBuffer,
    _: u64,
    done: CordisAbiCompletion,
) {
    finish(done, Default::default());
}
unsafe extern "C" fn defer(
    _: u64,
    _: u64,
    _: u64,
    _: CordisAbiSlice,
    _: u64,
    done: CordisAbiCompletion,
) {
    finish(done, Default::default());
}
unsafe extern "C" fn dispose(_: u64, _: u64, _: u64, done: CordisAbiCompletion) {
    finish(done, Default::default());
}
unsafe extern "C" fn resource(
    _: u64,
    _: u64,
    operation: u32,
    _: u64,
    _: CordisAbiSlice,
    _: CordisAbiSlice,
    _: *const CordisAbiBuffer,
    caller: u64,
    done: CordisAbiCompletion,
) {
    assert_eq!(caller, 2);
    let payload = if operation == 2 {
        Payload::message(Bytes::from_static(&[8, 42]))
    } else {
        Value::Null.into()
    };
    let bytes = payload.encode().unwrap();
    let buffer = CordisAbiBuffer {
        data: bytes.as_ptr(),
        len: bytes.len(),
    };
    let result = match operation {
        1 => CordisAbiResult {
            handle: 17,
            value: buffer,
            ..Default::default()
        },
        2 => CordisAbiResult {
            value: buffer,
            ..Default::default()
        },
        3 | 4 => CordisAbiResult {
            status: if operation == 3 { 1 } else { 2 },
            code: slice("failure"),
            message: slice("resource obligation remains"),
            ..Default::default()
        },
        _ => CordisAbiResult {
            reserved: 1,
            ..Default::default()
        },
    };
    finish(done, result);
}
fn finish(done: CordisAbiCompletion, raw: CordisAbiResult) {
    // SAFETY: this test host owns exactly one completion for each submission.
    unsafe {
        done.complete.unwrap()(done.user_data, raw);
    }
}
static HOST: CordisAbiHostApi = CordisAbiHostApi {
    version: 1,
    struct_size: mem::size_of::<CordisAbiHostApi>() as u32,
    invoke: Some(invoke),
    defer: Some(defer),
    dispose: Some(dispose),
    release_result: Some(release_host),
};
static RESOURCES: CordisAbiResourceHostApi = CordisAbiResourceHostApi {
    version: 1,
    struct_size: mem::size_of::<CordisAbiResourceHostApi>() as u32,
    call: Some(resource),
};
fn context() -> CordisAbiResourceContext {
    CordisAbiResourceContext {
        base: CordisAbiCompositionContext {
            base: CordisAbiContext {
                host: 1,
                grant: 2,
                api: &HOST,
            },
            ..Default::default()
        },
        resources: &RESOURCES,
    }
}
#[tokio::test]
async fn reverse_resource_results_release_exactly_once_for_every_outcome() {
    RELEASED.store(0, Ordering::SeqCst);
    // SAFETY: test-owned static tables and borrowed results match the C header.
    let host = unsafe { Host::read_resources(context()) }.unwrap();
    let call = |op| {
        worker::CALLER.scope(
            (1, 2),
            host.call(Call::Resource(
                op,
                0,
                String::new(),
                String::new(),
                Value::Null.into(),
            )),
        )
    };
    assert_eq!(call(1).await.unwrap().handle, 17);
    assert_eq!(
        call(2).await.unwrap().value.one().unwrap(),
        Bytes::from_static(&[8, 42])
    );
    assert!(matches!(
        call(3).await,
        Err(Failure::Rejected(_) | Failure::Refused(_))
    ));
    assert!(matches!(call(4).await, Err(Failure::Unknown(_))));
    assert!(matches!(call(5).await, Err(Failure::Unknown(_))));
    assert_eq!(RELEASED.load(Ordering::SeqCst), 5);
}
#[tokio::test]
async fn missing_or_malformed_resource_capability_is_rejected_before_calling_host() {
    let mut ctx = context();
    ctx.resources = std::ptr::null();
    // SAFETY: static base table, with an explicitly absent optional extension.
    let host = unsafe { Host::read_resources(ctx) }.unwrap();
    assert!(
        matches!(worker::CALLER.scope((1, 2), host.call(Call::Resource(1, 0, "dep".into(), "kind".into(), Value::Null.into()))).await, Err(Failure::Refused(e)) if e.code == "unsupported_resource")
    );
    let invalid = CordisAbiResourceHostApi {
        version: 2,
        ..RESOURCES
    };
    ctx.resources = &invalid;
    // SAFETY: table is readable but advertises a deliberately unsupported version.
    assert!(unsafe { Host::read_resources(ctx) }.is_err());
    let invalid = CordisAbiResourceHostApi {
        call: None,
        ..RESOURCES
    };
    ctx.resources = &invalid;
    // SAFETY: valid storage, with a deliberately missing required function.
    assert!(unsafe { Host::read_resources(ctx) }.is_err());
}
#[test]
fn resource_guest_table_requires_writable_complete_storage() {
    use cordis_portable_example::resources::CounterPlugin;
    let mut table = CordisAbiResourceApi::default();
    let size = mem::size_of::<CordisAbiResourceApi>();
    // SAFETY: writable initialized storage; rejected requests must leave it alone.
    unsafe {
        assert_eq!(resources_entry::<CounterPlugin>(1, &mut table, size - 1), 1);
        assert_eq!(
            resources_entry::<CounterPlugin>(1, std::ptr::null_mut(), size),
            1
        );
        assert_eq!(table.version, 0);
        assert!(table.dispatch.is_none());
        assert_eq!(resources_entry::<CounterPlugin>(1, &mut table, size), 0);
    }
    assert_eq!(table.struct_size as usize, size);
    assert!(table.dispatch.is_some());
}
#[test]
fn malformed_outcome_fields_are_unknown_and_release_exactly_once() {
    static RELEASES: AtomicUsize = AtomicUsize::new(0);
    unsafe extern "C" fn release(_: *mut CordisAbiResult) {
        RELEASES.fetch_add(1, Ordering::SeqCst);
    }
    let bytes = Payload::from(Value::Null).encode().unwrap();
    let buffer = CordisAbiBuffer {
        data: bytes.as_ptr(),
        len: bytes.len(),
    };
    let cases = [
        CordisAbiResult {
            value: buffer,
            code: slice("unexpected"),
            ..Default::default()
        },
        CordisAbiResult {
            status: 1,
            value: buffer,
            code: slice("business"),
            ..Default::default()
        },
        CordisAbiResult {
            status: 3,
            ..Default::default()
        },
        CordisAbiResult {
            status: 2,
            handle: 1,
            ..Default::default()
        },
    ];
    for (n, case) in cases.into_iter().enumerate() {
        assert!(matches!(
            consume(case, release, false, Validation::Checked),
            Err(Failure::Unknown(_))
        ));
        assert_eq!(RELEASES.load(Ordering::SeqCst), n + 1);
    }
}

#[tokio::test]
async fn reverse_carrier_audit_follows_pending_policy_and_releases_unknown_acquisitions_once() {
    unsafe extern "C" fn release(raw: *mut CordisAbiResult) {
        // SAFETY: this test keeps the result owner alive until completion returns.
        unsafe {
            (&*((*raw).owner.cast::<AtomicUsize>())).fetch_add(1, Ordering::SeqCst);
        }
    }
    let count = AtomicUsize::new(0);
    let mut bytes = Payload::message(Bytes::from_static(&[0xff, 0]))
        .encode()
        .unwrap()
        .to_vec();
    bytes.extend([0x78, 1]);
    let mut pending = Vec::new();
    for validation in [Validation::Contract, Validation::Audit, Validation::Checked] {
        let mut completion = None;
        let receive = submit_host(release, true, validation, |done| {
            completion = Some(done);
            Ok(())
        })
        .unwrap();
        pending.push((validation, completion.unwrap(), receive));
    }
    for (validation, completion, receive) in pending.into_iter().rev() {
        let before = count.load(Ordering::SeqCst);
        finish(
            completion,
            CordisAbiResult {
                value: CordisAbiBuffer {
                    data: bytes.as_ptr(),
                    len: bytes.len(),
                },
                handle: 17,
                owner: (&count as *const AtomicUsize).cast_mut().cast(),
                ..Default::default()
            },
        );
        let result = receive.await.unwrap();
        if validation.audits() {
            assert!(matches!(result, Err(Failure::Unknown(_))));
        } else {
            let reply = result.unwrap();
            assert_eq!(reply.handle, 17);
            assert_eq!(reply.value.one().unwrap().as_ref(), &[0xff, 0]);
        }
        assert_eq!(count.load(Ordering::SeqCst), before + 1);
    }
}
