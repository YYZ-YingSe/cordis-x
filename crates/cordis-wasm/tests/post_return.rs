mod support;
use cordis_sdk::*;
use cordis_wasm::{Component, Error as WasmError, Limits, WasmService, json};
use std::sync::{Arc, Mutex};
use wasmparser::{Parser, Payload, TypeRef};

// Corrupt only a real guest's selected canonical post-return body. All business
// code, resource creation, metadata and generated bindings remain unchanged.
fn trapping_post_return(export: &str) -> Component {
    let bytes = support::core_bytes();
    let mut imports = 0;
    let mut function = None;
    let mut bodies = Vec::new();
    let mut types = Vec::new();
    for payload in Parser::new(0).parse_all(bytes) {
        match payload.unwrap() {
            Payload::ImportSection(section) => {
                for import in section.into_imports() {
                    if matches!(import.unwrap().ty, TypeRef::Func(_) | TypeRef::FuncExact(_)) {
                        imports += 1;
                    }
                }
            }
            Payload::ExportSection(section) => {
                for item in section {
                    let item = item.unwrap();
                    if item.name == export {
                        function = Some(item.index);
                    }
                }
            }
            Payload::CodeSectionEntry(body) => bodies.push(body.range()),
            Payload::FunctionSection(section) => {
                types.extend(section.into_iter().map(|ty| ty.unwrap()))
            }
            _ => {}
        }
    }
    let target = (function.expect("generated post-return export") - imports) as usize;
    let mut module = wasm_encoder::Module::new();
    for payload in Parser::new(0).parse_all(bytes) {
        let payload = payload.unwrap();
        if let Payload::ExportSection(section) = payload {
            let mut exports = wasm_encoder::ExportSection::new();
            for item in section {
                let item = item.unwrap();
                let kind = match item.kind {
                    wasmparser::ExternalKind::Func => wasm_encoder::ExportKind::Func,
                    wasmparser::ExternalKind::Table => wasm_encoder::ExportKind::Table,
                    wasmparser::ExternalKind::Memory => wasm_encoder::ExportKind::Memory,
                    wasmparser::ExternalKind::Global => wasm_encoder::ExportKind::Global,
                    wasmparser::ExternalKind::Tag => wasm_encoder::ExportKind::Tag,
                    wasmparser::ExternalKind::FuncExact => {
                        unreachable!("wasmparser rejects exact function exports")
                    }
                };
                let index = if item.name == export {
                    imports + bodies.len() as u32
                } else {
                    item.index
                };
                exports.export(item.name, kind, index);
            }
            module.section(&exports);
        } else if matches!(payload, Payload::FunctionSection(_)) {
            let mut functions = wasm_encoder::FunctionSection::new();
            for ty in &types {
                functions.function(*ty);
            }
            functions.function(types[target]);
            module.section(&functions);
        } else if matches!(payload, Payload::CodeSectionStart { .. }) {
            let mut code = wasm_encoder::CodeSection::new();
            for range in &bodies {
                code.raw(&bytes[range.start as usize..range.end as usize]);
            }
            // Linkers can merge identical create/acquire post-return functions.
            // Retarget just this export to a new function instead of corrupting
            // the shared implementation used by other lifecycle operations.
            let mut trap = wasm_encoder::Function::new([]);
            trap.instruction(&wasm_encoder::Instruction::Unreachable)
                .instruction(&wasm_encoder::Instruction::End);
            code.function(&trap);
            module.section(&code);
        } else if let Some((id, range)) = payload.as_section() {
            module.section(&wasm_encoder::RawSection {
                id,
                data: &bytes[range.start as usize..range.end as usize],
            });
        }
    }
    Component::from_bytes(&support::package(&module.finish()), Limits::default()).unwrap()
}

async fn fails(stage: &str) {
    let export = format!("cabi_post_cordis:plugin/service@0.1.0#{stage}");
    let component = trapping_post_return(&export);
    let rt = Runtime::new();
    let service = Service::<WasmService>::new("example.calculator", "1").with_descriptor(
        cordis_contract::portable::decode(
            &cordis_test_contracts::resources::rust_fixture::SERVICE
                .parse()
                .unwrap(),
        )
        .unwrap(),
    );
    let provider = rt
        .install(component.plugin("wasm", &service, json!({})).unwrap())
        .unwrap();
    if stage == "create" {
        assert!(provider.ready().await.is_err());
    } else {
        provider.ready().await.unwrap();
        let saved = Arc::new(Mutex::new(None));
        let client = rt
            .install(
                Plugin::new("client", {
                    let service = service.clone();
                    let saved = saved.clone();
                    move |ctx| {
                        let service = service.clone();
                        let saved = saved.clone();
                        async move {
                            *saved.lock().unwrap() = Some(ctx.dependency(&service)?);
                            Ok(())
                        }
                    }
                })
                .requires(&service),
            )
            .unwrap();
        client.ready().await.unwrap();
        let handle = saved.lock().unwrap().take().unwrap();
        let acquire = stage.ends_with("acquire");
        let outcome = handle
            .call(move |s, c| {
                Box::pin(async move {
                    Ok(if acquire {
                        s.acquire_json(&c, "counter", json!({})).await.map(|_| ())
                    } else {
                        s.invoke_json(&c, "echo", json!(42)).await.map(|_| ())
                    })
                })
            })
            .await
            .unwrap();
        assert!(matches!(outcome, Err(WasmError::Unknown(_))));
        assert!(provider.dispose().await.is_err());
        if acquire {
            assert!(matches!(
                client.effects()[0].status,
                EffectStatus::Failed(_)
            ));
        }
    }
    assert!(provider.dispose().await.is_err());
    if stage.ends_with("acquire") {
        // A failed consumer inverse prevents provider recovery from starting.
        assert_eq!(provider.effects()[0].status, EffectStatus::Owned);
    } else {
        assert!(matches!(
            provider.effects()[0].status,
            EffectStatus::Failed(_)
        ));
    }
    assert!(rt.shutdown().await.is_err());
}
#[tokio::test]
async fn create_post_return_trap_preserves_unknown_instance() {
    fails("create").await;
}
#[tokio::test]
async fn acquire_post_return_trap_preserves_unknown_resource() {
    fails("[method]instance.acquire").await;
}
#[tokio::test]
async fn invoke_post_return_trap_faults_provider() {
    fails("[method]instance.invoke").await;
}
