#![cfg(unix)]
use cordis_abi::{AbiService, Library};
use cordis_sdk::*;
use std::{
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
struct Fixture(PathBuf);
impl Fixture {
    fn build(scenario: u32) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "cordis-resource-extension-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        // The raw C-table probe uses the current runtime message definition.
        // Do not freeze an older protocol-shaped descriptor in its C source.
        let interface = cordis_contract::portable::encode(
            &cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(false))
                .unwrap(),
        );
        let mut header = String::new();
        let values = [
            ("interface_payload", cordis_wire::Payload::from(interface)),
            (
                "context_payload",
                cordis_wire::Payload::from(cordis_wire::json!({"dependencies":{},"interfaces":{}})),
            ),
            (
                "null_payload",
                cordis_wire::Payload::from(cordis_wire::Value::Null),
            ),
            (
                "answer_payload",
                cordis_wire::Payload::message(
                    cordis_sdk_core::Message::from_json(
                        cordis_contract::MessageContract::json_value(),
                        &cordis_wire::json!(42),
                    )
                    .unwrap()
                    .encode(cordis_wire::MAX_MESSAGE)
                    .unwrap(),
                ),
            ),
            (
                "failed_payload",
                cordis_wire::Payload::message(
                    cordis_sdk_core::Message::from_json(
                        cordis_contract::MessageContract::json_value(),
                        &cordis_wire::json!(-1),
                    )
                    .unwrap()
                    .encode(cordis_wire::MAX_MESSAGE)
                    .unwrap(),
                ),
            ),
            (
                "argument_payload",
                cordis_wire::Payload::message(
                    cordis_sdk_core::Message::from_json(
                        cordis_contract::MessageContract::json_value(),
                        &cordis_wire::Value::Null,
                    )
                    .unwrap()
                    .encode(cordis_wire::MAX_MESSAGE)
                    .unwrap(),
                ),
            ),
        ];
        for (name, value) in values {
            let bytes = value.encode().unwrap();
            header.push_str(&format!(
                "static const unsigned char {name}[] = {{{}}};\n",
                bytes
                    .iter()
                    .map(u8::to_string)
                    .collect::<Vec<_>>()
                    .join(",")
            ));
        }
        std::fs::write(root.join("message_schema.h"), header).unwrap();
        let library = root.join(format!("fixture{}", std::env::consts::DLL_SUFFIX));
        let repository = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let output = std::process::Command::new("cc")
            .args(["-shared", "-fPIC", "-std=c11"])
            .arg(format!("-DSCENARIO={scenario}"))
            .arg("-I")
            .arg(repository.join("protocols/abi/include"))
            .arg("-I")
            .arg(&root)
            .arg(repository.join("crates/cordis-abi/tests/fixtures/resource_extension.c"))
            .arg("-o")
            .arg(&library)
            .output()
            .unwrap();
        assert!(
            output.status.success(),
            "{}",
            String::from_utf8_lossy(&output.stderr)
        );
        Self(library)
    }
    #[allow(unsafe_code)]
    fn load(&self) -> cordis_abi::Result<Library> {
        // SAFETY: the fixture is compiled from the repository's C header above.
        unsafe { Library::load(&self.0) }
    }
}
impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(self.0.parent().unwrap());
    }
}

#[test]
fn optional_resources_table_preserves_capability_limits_and_rejects_malformed_tables() {
    let basic = Fixture::build(0);
    assert!(!basic.load().unwrap().supports_dependency_resources());
    for mode in 2..=8 {
        assert!(
            Fixture::build(mode).load().is_err(),
            "invalid resource extension {mode}"
        );
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn canonical_c_table_runs_without_other_extensions_and_rejects_expired_or_unbound_authority()
{
    let fixture = Fixture::build(1);
    let library = fixture.load().unwrap();
    assert!(library.supports_dependency_resources());
    assert!(!library.supports_managed());
    assert!(!library.supports_reactive());
    assert!(!library.supports_composition());
    let runtime = Runtime::new();
    let service = Service::<AbiService>::new("test.resource-extension", "1").with_descriptor(
        cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(false))
            .unwrap(),
    );
    let provider = runtime
        .install(
            library
                .plugin("provider", &service, cordis_abi::Value::Null)
                .unwrap(),
        )
        .unwrap();
    provider.ready().await.unwrap();
    let binding = service.clone();
    let caller = runtime
        .install(
            Plugin::new("caller", move |ctx| {
                let binding = binding.clone();
                async move {
                    let result = ctx
                        .dependency(&binding)?
                        .call(|s, ctx| {
                            Box::pin(async move {
                                s.invoke_json(&ctx, "test", cordis_abi::Value::Null)
                                    .await
                                    .map_err(Into::into)
                            })
                        })
                        .await?;
                    assert_eq!(result, cordis_abi::json!(42.0));
                    Ok(())
                }
            })
            .requires(&service),
        )
        .unwrap();
    caller.ready().await.unwrap();
    runtime.shutdown().await.unwrap();
}
