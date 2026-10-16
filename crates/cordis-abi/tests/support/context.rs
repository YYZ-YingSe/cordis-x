#[path = "artifacts.rs"]
mod artifacts;
use cordis_abi::{AbiService, Capabilities, Dependency, Error as AbiError, Library, Value, json};
use cordis_sdk::*;
use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::Semaphore;

async fn bounded<T>(future: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(20), future)
        .await
        .expect("ABI Context stalled")
}
struct Files {
    root: PathBuf,
    binary: PathBuf,
}
impl Files {
    fn new(version: &str) -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!(
            "cordis-abi-context-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&root).unwrap();
        let binary = root.join(format!("fixture{}", std::env::consts::DLL_SUFFIX));
        std::fs::copy(artifacts::binary(version), &binary).unwrap();
        Self { root, binary }
    }
    fn load(&self) -> Library {
        // SAFETY: repository-owned C++ fixtures obey the native memory contract.
        #[allow(unsafe_code)]
        unsafe {
            Library::load(&self.binary).unwrap()
        }
    }
    fn config(&self, mut config: Value) -> Value {
        config["trace"] = json!(self.root.join("trace"));
        config
    }
    fn count(&self, event: &str) -> usize {
        std::fs::read_to_string(self.root.join("trace"))
            .unwrap_or_default()
            .lines()
            .filter(|e| *e == event)
            .count()
    }
    async fn wait(&self, event: &str) {
        bounded(async {
            while self.count(event) == 0 {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        })
        .await;
    }
}
impl Drop for Files {
    fn drop(&mut self) {
        // Keep failed-recovery evidence beside intentionally retained code.
        if self.count("create") == 0 || self.count("unload") != 0 {
            let _ = std::fs::remove_dir_all(&self.root);
        }
    }
}
#[derive(Clone, Debug)]
struct Record {
    method: String,
    args: Value,
    owner: cordis_kernel::Activation,
    caller: Option<cordis_kernel::Activation>,
    cleanup: bool,
}
struct Logger {
    records: Mutex<Vec<Record>>,
    entered: Semaphore,
    gate: Semaphore,
    reenter: Mutex<Option<ServiceHandle<AbiService>>>,
}
impl Logger {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            records: Mutex::default(),
            entered: Semaphore::new(0),
            gate: Semaphore::new(0),
            reenter: Mutex::default(),
        })
    }
    fn records(&self, method: &str) -> Vec<Record> {
        self.records
            .lock()
            .unwrap()
            .iter()
            .filter(|r| r.method == method)
            .cloned()
            .collect()
    }
}
fn logger_service() -> Service<Logger> {
    Service::new("example.logger", "1")
}
fn caps() -> Capabilities {
    Capabilities {
        dependencies: vec![Dependency::new_json(
            &logger_service(), cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(true)).unwrap(),
            |logger, ctx, method, args| {
                Box::pin(async move {
                    logger.records.lock().unwrap().push(Record {
                        method: method.clone(),
                        args: args.clone(),
                        owner: ctx.owner(),
                        caller: ctx.caller(),
                        cleanup: ctx.is_cleanup(),
                    });
                    if method == "hold"
                        || method == "cleanup"
                            && args.as_str().is_some_and(|s| s.ends_with(":hold"))
                    {
                        logger.entered.add_permits(1);
                        logger.gate.acquire().await.unwrap().forget();
                    }
                    if method == "reenter" {
                        let handle = logger.reenter.lock().unwrap().clone().unwrap();
                        return invoke_json(&handle, "echo", args).await;
                    }
                    Ok(args)
                })
            },
        )],
        ..Default::default()
    }
}
async fn install_logger(rt: &Runtime, logger: Arc<Logger>, view: View) -> PluginHandle {
    let service = logger_service();
    let declaration = service.clone();
    let plugin = rt
        .install_in(
            view,
            Plugin::new("logger", move |ctx| {
                let logger = logger.clone();
                let service = service.clone();
                async move { ctx.provide(&service, logger) }
            })
            .provides(&declaration),
        )
        .unwrap();
    bounded(plugin.ready()).await.unwrap();
    plugin
}
async fn client<T: Send + Sync + 'static>(
    rt: &Runtime,
    service: &Service<T>,
) -> (PluginHandle, ServiceHandle<T>) {
    let saved = Arc::new(Mutex::new(None));
    let plugin = rt
        .install(
            Plugin::new("client", {
                let saved = saved.clone();
                let service = service.clone();
                move |ctx| {
                    let saved = saved.clone();
                    let service = service.clone();
                    async move {
                        *saved.lock().unwrap() = Some(ctx.dependency(&service)?);
                        Ok(())
                    }
                }
            })
            .requires(service),
        )
        .unwrap();
    bounded(plugin.ready()).await.unwrap();
    let handle = saved.lock().unwrap().take().unwrap();
    (plugin, handle)
}
async fn invoke_json(
    handle: &ServiceHandle<AbiService>,
    method: &str,
    args: Value,
) -> cordis_abi::Result<Value> {
    let method = method.to_owned();
    handle
        .call(move |service, ctx| {
            Box::pin(async move { Ok(service.invoke_json(&ctx, &method, args).await) })
        })
        .await?
}
struct Rig {
    rt: Runtime,
    provider: PluginHandle,
    client: PluginHandle,
    handle: ServiceHandle<AbiService>,
    logger: Arc<Logger>,
}
async fn rig(files: &Files, config: Value, capabilities: Capabilities) -> Rig {
    let rt = Runtime::new();
    let logger = Logger::new();
    install_logger(&rt, logger.clone(), View::new()).await;
    let library = files.load().with_context(capabilities).unwrap();
    let service = Service::new("example.context", "1").with_descriptor(cordis_contract::Descriptor::new(cordis_contract::Interface::value_dispatch(true)).unwrap());
    let provider = rt
        .install(
            library
                .plugin("context", &service, files.config(config))
                .unwrap(),
        )
        .unwrap();
    bounded(provider.ready()).await.unwrap();
    let (client, handle) = client(&rt, &service).await;
    Rig {
        rt,
        provider,
        client,
        handle,
        logger,
    }
}


#[allow(dead_code)]
fn wire_json(value: Value) -> cordis_abi::Bytes {
    cordis_sdk_core::Message::from_json(cordis_contract::MessageContract::json_value(),&value).unwrap().encode(cordis_wire::MAX_MESSAGE).unwrap()
}
#[allow(dead_code)]
fn unwire_json(value: &cordis_abi::Bytes) -> Value {
    cordis_sdk_core::Message::from_bytes(cordis_contract::MessageContract::json_value(),value.clone()).unwrap().to_json().unwrap()
}
