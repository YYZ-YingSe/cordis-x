use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
unsafe extern "C" fn release(raw: *mut RawResult) {
    // SAFETY: each test supplies a live AtomicUsize as the ownership record.
    unsafe {
        (&*((*raw).owner.cast::<AtomicUsize>())).fetch_add(1, Ordering::SeqCst);
    }
}
#[test]
fn foreign_result_released_once_on_success_failure_and_malformed_output() {
    let count = AtomicUsize::new(0);
    let payload = Payload::message(Bytes::from_static(&[0, 255, 0, 128]));
    let bytes = payload.encode().unwrap();
    for (status, malformed, accepted) in [
        (0, false, true),
        (0, true, false),
        (1, false, false),
        (2, false, false),
        (3, false, false),
    ] {
        let raw = RawResult {
            status,
            value: if malformed {
                RawBuffer {
                    data: std::ptr::null(),
                    len: 1,
                }
            } else {
                RawBuffer {
                    data: bytes.as_ptr(),
                    len: bytes.len(),
                }
            },
            code: if status == 0 {
                Slice::default()
            } else {
                Slice::new("failure")
            },
            owner: (&count as *const AtomicUsize).cast_mut().cast(),
            ..RawResult::default()
        };
        let before = count.load(Ordering::SeqCst);
        let result = finish(raw, release, false, Validation::Checked);
        assert_eq!(result.is_ok(), accepted);
        assert_eq!(count.load(Ordering::SeqCst), before + 1);
        if accepted {
            assert_eq!(result.unwrap().value, payload);
        }
    }
}
#[test]
fn copied_business_body_outlives_foreign_buffer() {
    let count = AtomicUsize::new(0);
    let value = {
        let bytes = Payload::message(Bytes::from_static(&[0, 255, 1]))
            .encode()
            .unwrap();
        finish(
            RawResult {
                value: RawBuffer {
                    data: bytes.as_ptr(),
                    len: bytes.len(),
                },
                owner: (&count as *const AtomicUsize).cast_mut().cast(),
                ..RawResult::default()
            },
            release,
            false,
            Validation::Checked,
        )
        .unwrap()
        .value
    };
    assert_eq!(value.one().unwrap().as_ref(), &[0, 255, 1]);
    assert_eq!(count.load(Ordering::SeqCst), 1);
}
#[test]
fn rejects_invalid_buffer_before_dereferencing_and_preserves_acquisition_uncertainty() {
    for raw in [
        RawBuffer {
            data: std::ptr::null(),
            len: 1,
        },
        RawBuffer {
            data: std::ptr::dangling(),
            len: cordis_wire::MAX_MESSAGE + 1,
        },
    ] {
        assert!(unsafe { read_payload(&raw, &mut Budget::new(), Validation::Checked) }.is_err());
    }
    let count = AtomicUsize::new(0);
    let bytes = Payload::from(Value::Null).encode().unwrap();
    for (acquisition, handle) in [(true, 0), (false, 1)] {
        assert!(matches!(
            finish(
                RawResult {
                    value: RawBuffer {
                        data: bytes.as_ptr(),
                        len: bytes.len()
                    },
                    handle,
                    owner: (&count as *const AtomicUsize).cast_mut().cast(),
                    ..RawResult::default()
                },
                release,
                acquisition,
                Validation::Checked
            ),
            Err(Error::Unknown(_))
        ));
    }
    assert_eq!(count.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn carrier_audit_is_owned_by_each_pending_completion_and_releases_once() {
    let count = AtomicUsize::new(0);
    let mut bytes = Payload::message(Bytes::from_static(&[0xff, 0]))
        .encode()
        .unwrap()
        .to_vec();
    bytes.extend([0x78, 1]);
    let mut pending = Vec::new();
    for validation in [Validation::Contract, Validation::Audit, Validation::Checked] {
        let mut completion = None;
        let receive = submit(release, true, validation, |done| {
            completion = Some(done);
            Ok(())
        })
        .unwrap();
        pending.push((validation, completion.unwrap(), receive));
    }
    for (validation, completion, receive) in pending.into_iter().rev() {
        let before = count.load(Ordering::SeqCst);
        // SAFETY: this invocation owns one completion and readable result storage.
        unsafe {
            completion.deliver(RawResult {
                value: RawBuffer {
                    data: bytes.as_ptr(),
                    len: bytes.len(),
                },
                handle: 17,
                owner: (&count as *const AtomicUsize).cast_mut().cast(),
                ..Default::default()
            });
        }
        let result = receive.await.unwrap();
        if validation.audits() {
            assert!(matches!(result, Err(Error::Unknown(_))));
        } else {
            let reply = result.unwrap();
            assert_eq!(reply.handle, 17);
            assert_eq!(reply.value.one().unwrap().as_ref(), &[0xff, 0]);
        }
        assert_eq!(count.load(Ordering::SeqCst), before + 1);
    }
}
