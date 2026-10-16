use crate::{AbiService, Bytes, Error, Instance, Payload, Result, Value, ffi::Op};
use cordis_runtime::{BoxFuture, Context, Plugin, Service, WeakEffectHandle};
use cordis_wire::local::Envelope;
use std::{
    collections::BTreeMap,
    fmt,
    sync::{
        Arc, Mutex, OnceLock, Weak,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::sync::Semaphore;

type Acquire = dyn Fn(Context, String, Envelope) -> BoxFuture<'static, Result<crate::AbiResource>>
    + Send
    + Sync;
type Invoke =
    dyn Fn(Context, String, Envelope) -> BoxFuture<'static, Result<Envelope>> + Send + Sync;
/// Explicit typed adapter for a dependency declared by the native library.
#[derive(Clone)]
pub struct Dependency {
    declaration: Service<()>,
    descriptor: Option<cordis_contract::Descriptor>,
    invoke: Arc<Invoke>,
    pub(crate) acquire: Option<Arc<Acquire>>,
}
impl fmt::Debug for Dependency {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_tuple("Dependency")
            .field(self.declaration.contract())
            .finish()
    }
}
impl Dependency {
    pub(crate) fn contract(&self) -> &cordis_contract::ContractId {
        self.declaration.contract()
    }
    pub(crate) fn descriptor(&self) -> &cordis_contract::Descriptor {
        self.descriptor.as_ref().expect("validated binding")
    }
    pub fn new<T, F>(
        service: &Service<T>,
        descriptor: cordis_contract::Descriptor,
        invoke: F,
    ) -> Self
    where
        T: ?Sized + Send + Sync + 'static,
        F: for<'a> Fn(&'a T, Context, String, Bytes) -> BoxFuture<'a, Result<Bytes>>
            + Send
            + Sync
            + 'static,
    {
        let invoke = Arc::new(invoke);
        Self::bind(
            service,
            Some(descriptor),
            move |service, ctx, method, args| {
                let invoke = invoke.clone();
                Box::pin(async move {
                    let bytes = invoke(
                        service,
                        ctx,
                        method,
                        args.one().map_err(|e| Error::InvalidInput(e.to_string()))?,
                    )
                    .await?;
                    Ok(Payload::message(bytes).into())
                })
            },
        )
    }
    /// Explicit JSON gateway for a host application whose service API is JSON.
    pub fn new_json<T, F>(
        service: &Service<T>,
        descriptor: cordis_contract::Descriptor,
        invoke: F,
    ) -> Self
    where
        T: ?Sized + Send + Sync + 'static,
        F: for<'a> Fn(&'a T, Context, String, Value) -> BoxFuture<'a, Result<Value>>
            + Send
            + Sync
            + 'static,
    {
        let schema = descriptor.clone();
        let invoke = Arc::new(invoke);
        Self::new(service, descriptor, move |value, ctx, method, args| {
            let schema = schema.clone();
            let invoke = invoke.clone();
            Box::pin(async move {
                let signature =
                    cordis_contract::portable::method(&schema.interface().methods, &method)
                        .map_err(Error::InvalidInput)?;
                let input = crate::body_json(&signature.args, &args)
                    .map_err(|e| Error::InvalidInput(e.to_string()))?;
                let result = invoke(value, ctx, method, input).await?;
                crate::json_body(&signature.result, &result)
                    .map_err(|e| Error::Unknown(e.to_string()))
            })
        })
    }
    fn bind<T, F>(
        service: &Service<T>,
        descriptor: Option<cordis_contract::Descriptor>,
        invoke: F,
    ) -> Self
    where
        T: ?Sized + Send + Sync + 'static,
        F: for<'a> Fn(&'a T, Context, String, Envelope) -> BoxFuture<'a, Result<Envelope>>
            + Send
            + Sync
            + 'static,
    {
        let declaration = Service::new(
            service.contract().name.clone(),
            service.contract().revision.clone(),
        );
        let service = service.clone();
        let invoke = Arc::new(invoke);
        Self {
            declaration,
            descriptor,
            acquire: None,
            invoke: Arc::new(move |ctx, method, args| {
                let service = service.clone();
                let invoke = invoke.clone();
                Box::pin(async move {
                    let dependency = ctx.dependency(&service)?;
                    dependency
                        .call(move |value, ctx| {
                            Box::pin(async move { Ok(invoke(value, ctx, method, args).await) })
                        })
                        .await?
                })
            }),
        }
    }
    /// Bind both value calls and caller-owned resources from an ABI service.
    pub fn abi(service: &Service<AbiService>) -> Self {
        let mut dependency = Self::bind(
            service,
            service.descriptor().cloned(),
            |service, ctx, method, args| {
                Box::pin(async move { service.invoke_envelope(&ctx, &method, args).await })
            },
        );
        let service = service.clone();
        dependency.acquire = Some(Arc::new(move |ctx, kind, args| {
            let service = service.clone();
            Box::pin(async move {
                ctx.dependency(&service)?
                    .call(move |service, ctx| {
                        Box::pin(
                            async move { Ok(service.acquire_envelope(&ctx, &kind, args).await) },
                        )
                    })
                    .await?
            })
        }));
        dependency
    }
}

/// Host policy, checked against the extension manifest before instance creation.
/// Effect capacity includes terminal tombstones to preserve cached outcomes.
#[derive(Clone, Debug)]
pub struct Capabilities {
    pub validation: cordis_contract::Validation,
    pub dependencies: Vec<Dependency>,
    pub max_effects: usize,
    pub max_contexts: usize,
    pub max_callbacks: usize,
    pub managed: crate::ManagedLimits,
    pub events: Vec<crate::EventBinding>,
    pub middleware: Vec<crate::MiddlewareBinding>,
    pub reactive: crate::ReactiveLimits,
    pub factories: Vec<crate::FactoryBinding>,
    pub provisions: Vec<crate::ProvisionBinding>,
    pub composition: crate::CompositionLimits,
}
impl Default for Capabilities {
    fn default() -> Self {
        Self {
            validation: cordis_contract::Validation::Checked,
            dependencies: vec![],
            max_effects: 1024,
            max_contexts: 256,
            max_callbacks: 16,
            managed: crate::ManagedLimits::default(),
            events: vec![],
            middleware: vec![],
            reactive: crate::ReactiveLimits::default(),
            factories: vec![],
            provisions: vec![],
            composition: crate::CompositionLimits::default(),
        }
    }
}
impl Capabilities {
    pub(crate) fn validate(
        &self,
        manifest: Option<&Value>,
        interfaces: Option<&BTreeMap<String, cordis_contract::Descriptor>>,
    ) -> Result<()> {
        if self.max_contexts == 0
            || self.max_callbacks == 0
            || self.max_callbacks > Semaphore::MAX_PERMITS
        {
            return Err(Error::InvalidInput("invalid Context capacity".into()));
        }
        self.managed.validate()?;
        let mut expected = cordis_wire::json!({});
        for dependency in &self.dependencies {
            let contract = dependency.declaration.contract();
            let actual = dependency.descriptor.as_ref().ok_or_else(|| {
                Error::Contract(format!("dependency {contract}: descriptor required"))
            })?;
            let required = interfaces
                .and_then(|m| m.get(contract.name.as_ref()))
                .ok_or_else(|| {
                    Error::Contract(format!("dependency {contract}: declaration missing"))
                })?;
            required
                .compare(actual)
                .map_err(|e| Error::Contract(format!("dependencies.{contract}: {e}")))?;
            if expected
                .as_object_mut()
                .unwrap()
                .insert(
                    contract.name.to_string(),
                    Value::String(contract.revision.to_string()),
                )
                .is_some()
            {
                return Err(Error::Contract("duplicate ABI dependency".into()));
            }
        }
        if manifest.unwrap_or(&cordis_wire::json!({})) != &expected {
            return Err(Error::Contract(
                "ABI Context dependencies must exactly match the guest manifest".into(),
            ));
        }
        Ok(())
    }
    pub(crate) fn declare(&self, mut plugin: Plugin) -> Plugin {
        for dependency in &self.dependencies {
            plugin = plugin.requires(&dependency.declaration);
        }
        for binding in &self.provisions {
            plugin = plugin.provides(&binding.service);
        }
        plugin
    }
}

pub(crate) fn next_id() -> Result<u64> {
    static NEXT: AtomicU64 = AtomicU64::new(1);
    NEXT.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
        .map_err(|_| Error::Overloaded)
}
fn registry() -> &'static Mutex<BTreeMap<u64, Weak<Router>>> {
    static REGISTRY: OnceLock<Mutex<BTreeMap<u64, Weak<Router>>>> = OnceLock::new();
    REGISTRY.get_or_init(Mutex::default)
}
#[derive(Clone, Copy)]
pub(crate) struct Execution {
    pub caller: u64,
    pub cleanup: bool,
    pub callback: Option<(u32, u64)>,
}
tokio::task_local! { pub(crate) static EXECUTION: Execution; }
pub(crate) fn in_cleanup() -> bool {
    EXECUTION.try_with(|call| call.cleanup).unwrap_or(false)
}
pub(crate) fn active_callback() -> Option<(u32, u64)> {
    EXECUTION.try_with(|call| call.callback).ok().flatten()
}
struct CallState {
    open: bool,
    calls: usize,
}
struct CallScope {
    context: Context,
    lane: u32,
    execution: Execution,
    state: Mutex<CallState>,
    drained: tokio::sync::Notify,
}
pub(crate) struct HostCall {
    scope: Arc<CallScope>,
    // A callback into the plugin can still be executing after it signals the
    // receiver. Keep its code loaded until that C completion returns.
    _instance: Arc<Instance>,
}
impl HostCall {
    pub fn execution(&self) -> Execution {
        self.scope.execution
    }
}
impl Drop for HostCall {
    fn drop(&mut self) {
        let last = {
            let mut state = self.scope.state.lock().unwrap();
            state.calls -= 1;
            state.calls == 0
        };
        if last {
            self.scope.drained.notify_waiters();
        }
    }
}

pub(crate) struct Router {
    pub id: u64,
    pub runtime: tokio::runtime::Handle,
    pub(crate) instance: Weak<Instance>,
    pub(crate) capabilities: Arc<Capabilities>,
    contexts: Mutex<BTreeMap<u64, Arc<CallScope>>>,
    pub(crate) managed: crate::managed::Managed,
    pub(crate) reactive: crate::reactive::Reactive,
    pub(crate) composition: crate::composition::Composition,
    pub(crate) resources: crate::resource_dependencies::Resources,
    effects: Mutex<BTreeMap<u64, Arc<Entry>>>,
    calls: Semaphore,
    cleanup_calls: Semaphore,
    disposals: Semaphore,
}
struct Entry {
    callback: u64,
    handle: OnceLock<WeakEffectHandle>,
    result: Mutex<Option<Result<()>>>,
}
impl Router {
    pub fn new(instance: Weak<Instance>, capabilities: Arc<Capabilities>, id: u64) -> Arc<Self> {
        let mut bound = (*capabilities).clone();
        for dependency in &mut bound.dependencies {
            dependency.descriptor = dependency
                .descriptor
                .take()
                .map(|d| d.with_validation(bound.validation));
        }
        let capabilities = Arc::new(bound);
        let managed =
            crate::managed::Managed::new(capabilities.max_callbacks.min(capabilities.max_contexts));
        let reactive = crate::reactive::Reactive::new(&capabilities);
        let composition = crate::composition::Composition::new(&capabilities);
        let resources = crate::resource_dependencies::Resources::new(capabilities.max_callbacks);
        let router = Arc::new(Self {
            id,
            runtime: tokio::runtime::Handle::current(),
            instance,
            calls: Semaphore::new(capabilities.max_callbacks),
            cleanup_calls: Semaphore::new(capabilities.max_callbacks),
            disposals: Semaphore::new(capabilities.max_callbacks),
            capabilities,
            contexts: Mutex::default(),
            managed,
            reactive,
            composition,
            resources,
            effects: Mutex::default(),
        });
        registry()
            .lock()
            .unwrap()
            .insert(id, Arc::downgrade(&router));
        router
    }
    pub fn allocate_id() -> Result<u64> {
        next_id()
    }
    pub fn get(id: u64) -> Result<Arc<Self>> {
        registry()
            .lock()
            .unwrap()
            .get(&id)
            .and_then(Weak::upgrade)
            .ok_or(Error::Closed)
    }
    pub fn scope(self: &Arc<Self>, ctx: &Context, op: Op) -> Result<Scope> {
        let kind = if ctx.is_cleanup() {
            1
        } else {
            match op {
                Op::Managed(8, _) => 2,
                Op::Managed(11, _) => 3,
                Op::Managed(12, _) => 4,
                Op::Managed(13, _) => 5,
                Op::Managed(14, _) => 6,
                Op::Managed(15, _) => 7,
                _ => 0,
            }
        };
        let mut contexts = self.contexts.lock().unwrap();
        // Reserve independent admission for inverses even when ordinary calls
        // have filled their Context grant budget.
        if contexts.values().filter(|entry| entry.lane == kind).count()
            >= self.capabilities.max_contexts
        {
            return Err(Error::Overloaded);
        }
        let grant = next_id()?;
        contexts.insert(
            grant,
            Arc::new(CallScope {
                context: ctx.snapshot(),
                lane: kind,
                execution: Execution {
                    caller: grant,
                    cleanup: ctx.is_cleanup(),
                    callback: match op {
                        Op::Managed(operation, target) => Some((operation, target)),
                        _ => None,
                    },
                },
                state: Mutex::new(CallState {
                    open: true,
                    calls: 0,
                }),
                drained: tokio::sync::Notify::new(),
            }),
        );
        Ok(Scope {
            router: self.clone(),
            grant,
            finished: false,
        })
    }
    pub(crate) fn admit(&self, caller: u64) -> Result<HostCall> {
        let scope = self
            .contexts
            .lock()
            .unwrap()
            .get(&caller)
            .cloned()
            .ok_or(Error::Closed)?;
        let instance = self.instance.upgrade().ok_or(Error::Closed)?;
        {
            let mut state = scope.state.lock().unwrap();
            if !state.open {
                return Err(Error::Closed);
            }
            state.calls = state.calls.checked_add(1).ok_or(Error::Overloaded)?;
        }
        Ok(HostCall {
            scope,
            _instance: instance,
        })
    }
    pub(crate) fn context(&self, grant: u64) -> Result<Context> {
        let scope = self
            .contexts
            .lock()
            .unwrap()
            .get(&grant)
            .cloned()
            .ok_or(Error::Closed)?;
        // Work admitted before the guest completed still owns its Context.
        if !scope.state.lock().unwrap().open
            && EXECUTION
                .try_with(|call| call.caller != grant)
                .unwrap_or(true)
        {
            return Err(Error::Closed);
        }
        Ok(scope.context.clone())
    }
    pub(crate) fn task_context(&self, grant: u64) -> Result<Context> {
        if self
            .contexts
            .lock()
            .unwrap()
            .get(&grant)
            .is_none_or(|entry| entry.lane != 4)
        {
            return Err(Error::Closed);
        }
        self.context(grant)
    }
    pub async fn invoke(
        &self,
        grant: u64,
        name: String,
        method: String,
        args: Bytes,
    ) -> Result<Bytes> {
        let ctx = self.context(grant)?;
        let capacity = if ctx.is_cleanup() {
            &self.cleanup_calls
        } else {
            &self.calls
        };
        let _permit = capacity.try_acquire().map_err(|_| Error::Overloaded)?;
        let dependency = self
            .capabilities
            .dependencies
            .iter()
            .find(|d| d.declaration.contract().name.as_ref() == name)
            .ok_or_else(|| Error::Refused {
                code: "undeclared_dependency".into(),
                message: name,
            })?;
        let invoke = dependency.invoke.clone();
        let descriptor = dependency.descriptor.as_ref().expect("validated binding");
        let signature = cordis_contract::portable::method(&descriptor.interface().methods, &method)
            .map_err(Error::InvalidInput)?
            .clone();
        let mut args = Envelope::from(Payload::message(args));
        args.check(&signature.args).map_err(Error::InvalidInput)?;
        let instance = self.instance.clone();
        ctx.operation(move |ctx| async move {
            let outcome = invoke(ctx.clone(), method, args)
                .await
                .map_err(|e| e.check_declared(&signature.errors, "dependency errors"))
                .and_then(|mut value| {
                    value.check(&signature.result).map_err(Error::Unknown)?;
                    value.one().map_err(|e| Error::Unknown(e.to_string()))
                });
            if let Err(error @ Error::Unknown(_)) = &outcome {
                if let Some(instance) = instance.upgrade() {
                    instance.fault(error.clone());
                }
                let _ = ctx.fail(error.to_string());
            }
            Ok(outcome)
        })
        .await?
    }
    pub fn defer(&self, grant: u64, callback: u64, label: String) -> Result<u64> {
        if callback == 0 || label.len() > 1024 {
            return Err(Error::InvalidInput(
                "invalid effect callback or label".into(),
            ));
        }
        let ctx = self.context(grant)?;
        let instance = self.instance.upgrade().ok_or(Error::Closed)?;
        let mut entries = self.effects.lock().unwrap();
        if entries.values().any(|entry| entry.callback == callback) {
            return Err(Error::InvalidInput("duplicate effect callback".into()));
        }
        if entries.len() >= self.capabilities.max_effects {
            return Err(Error::Overloaded);
        }
        let id = next_id()?;
        let entry = Arc::new(Entry {
            callback,
            handle: OnceLock::new(),
            result: Mutex::new(None),
        });
        let cleanup_entry = entry.clone();
        // Native Effect holds the instance and definition/use-site ownership.
        // Registry stores only a weak handle, so it does not create an owner cycle.
        let handle = ctx.defer(label, move |cleanup| async move {
            let result = instance
                .run_cleanup(cleanup, Op::EffectRelease(callback))
                .await;
            *cleanup_entry.result.lock().unwrap() = Some(result.clone());
            result.map_err(Into::into)
        })?;
        let _ = entry.handle.set(handle.downgrade());
        entries.insert(id, entry);
        Ok(id)
    }
    pub async fn dispose(&self, id: u64) -> Result<()> {
        let _permit = self
            .disposals
            .try_acquire()
            .map_err(|_| Error::Overloaded)?;
        let entry = self
            .effects
            .lock()
            .unwrap()
            .get(&id)
            .cloned()
            .ok_or(Error::Closed)?;
        if let Some(handle) = entry.handle.get().and_then(WeakEffectHandle::upgrade) {
            if let Err(error) = handle.dispose().await {
                return Err(entry
                    .result
                    .lock()
                    .unwrap()
                    .as_ref()
                    .and_then(|r| r.clone().err())
                    .unwrap_or_else(|| error.into()));
            }
            return Ok(());
        }
        entry
            .result
            .lock()
            .unwrap()
            .clone()
            .unwrap_or(Err(Error::Closed))
    }
    pub fn recovered(&self) -> bool {
        self.managed.recovered()
            && self.reactive.recovered()
            && self
                .effects
                .lock()
                .unwrap()
                .values()
                .all(|entry| matches!(*entry.result.lock().unwrap(), Some(Ok(()))))
    }
}
impl Drop for Router {
    fn drop(&mut self) {
        registry().lock().unwrap().remove(&self.id);
    }
}
pub(crate) struct Scope {
    pub router: Arc<Router>,
    pub grant: u64,
    finished: bool,
}
impl Scope {
    pub async fn finish(mut self) {
        let scope = self
            .router
            .contexts
            .lock()
            .unwrap()
            .get(&self.grant)
            .cloned()
            .expect("live scope");
        scope.state.lock().unwrap().open = false;
        loop {
            let ready = scope.drained.notified();
            tokio::pin!(ready);
            ready.as_mut().enable();
            if scope.state.lock().unwrap().calls == 0 {
                break;
            }
            ready.await;
        }
        self.finished = true;
    }
}
impl Drop for Scope {
    fn drop(&mut self) {
        let scope = self.router.contexts.lock().unwrap().remove(&self.grant);
        if let Some(scope) = scope {
            scope.state.lock().unwrap().open = false;
            if !self.finished {
                let error = Error::Unknown("ABI callback scope abandoned before draining".into());
                if let Some(instance) = self.router.instance.upgrade() {
                    instance.fault(error.clone());
                }
                let _ = scope.context.fail(error.to_string());
            }
        }
    }
}
