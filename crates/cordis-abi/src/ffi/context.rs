//! Optional Context table and host-owned callback results. No borrowed Rust
//! pointers escape; stale tokens resolve through the safe registry.
use super::*;
use crate::context::{Router, Scope};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::ptr;
mod resources;

#[repr(C)]
struct HostApi {
    version: u32,
    size: u32,
    invoke: unsafe extern "C" fn(u64, u64, Slice, Slice, *const RawBuffer, u64, Completion),
    defer: unsafe extern "C" fn(u64, u64, u64, Slice, u64, Completion),
    dispose: unsafe extern "C" fn(u64, u64, u64, Completion),
    release: Release,
}
#[repr(C)]
struct RawContext {
    host: u64,
    grant: u64,
    api: *const HostApi,
}
type Dispatch =
    unsafe extern "C" fn(u64, u32, u64, RawContext, Slice, *const RawBuffer, Completion);
#[repr(C)]
#[derive(Default)]
struct ContextApi {
    version: u32,
    size: u32,
    describe: Option<unsafe extern "C" fn() -> RawResult>,
    dispatch: Option<Dispatch>,
}
pub(crate) struct Extension {
    dispatch: Dispatch,
    pub dependencies: Value,
    pub interfaces: std::collections::BTreeMap<String, cordis_contract::Descriptor>,
    managed: Option<ManagedDispatch>,
    reactive: Option<ReactiveExtension>,
    composition: Option<CompositionExtension>,
    resources: Option<resources::Dispatch>,
}
impl Extension {
    pub(super) unsafe fn load(
        library: &libloading::Library,
        release: Release,
    ) -> Result<Option<Self>> {
        type Entry = unsafe extern "C" fn(u32, *mut ContextApi, usize) -> u32;
        // SAFETY: caller trusts this module and the optional exported signature.
        let Ok(entry) = (unsafe { library.get::<Entry>(b"cordis_abi_context_v1\0") }) else {
            // A Managed table cannot be interpreted without Context v1.
            if unsafe { library.get::<unsafe extern "C" fn()>(b"cordis_abi_managed_v1\0") }.is_ok()
                || unsafe { library.get::<unsafe extern "C" fn()>(b"cordis_abi_reactive_v1\0") }
                    .is_ok()
                || unsafe { library.get::<unsafe extern "C" fn()>(b"cordis_abi_composition_v1\0") }
                    .is_ok()
                || unsafe { library.get::<unsafe extern "C" fn()>(b"cordis_abi_resources_v1\0") }
                    .is_ok()
            {
                return Err(Error::Load("ABI extension requires Context v1".into()));
            }
            return Ok(None);
        };
        let mut api = ContextApi::default();
        // SAFETY: writable exact-size table owned by this stack frame.
        let status = unsafe { entry(1, &mut api, mem::size_of::<ContextApi>()) };
        let invalid = || Error::Load("unsupported or incomplete ABI Context v1 table".into());
        if status != 0 || api.version != 1 || api.size as usize != mem::size_of::<ContextApi>() {
            return Err(invalid());
        }
        let describe = api.describe.ok_or_else(invalid)?;
        let dispatch = api.dispatch.ok_or_else(invalid)?;
        // SAFETY: result follows base v1 ownership; finish releases even on error.
        let description = finish(unsafe { describe() }, release, false, Validation::Audit)?
            .value
            .control_only()
            .map_err(|e| Error::Load(e.to_string()))?;
        if description.as_object().map(|m| m.len()) != Some(2) {
            return Err(invalid());
        }
        let dependencies = description["dependencies"].clone();
        let interfaces = description["interfaces"]
            .as_object()
            .ok_or_else(invalid)?
            .iter()
            .map(|(name, value)| {
                Ok((
                    name.clone(),
                    cordis_contract::portable::decode(value).map_err(Error::Contract)?,
                ))
            })
            .collect::<Result<std::collections::BTreeMap<_, _>>>()?;
        if dependencies
            .as_object()
            .is_none_or(|d| d.keys().ne(interfaces.keys()))
        {
            return Err(invalid());
        }
        if !dependencies.as_object().is_some_and(|map| {
            map.iter().all(|(name, revision)| {
                !name.is_empty() && revision.as_str().is_some_and(|s| !s.is_empty())
            })
        }) {
            return Err(invalid());
        }
        // SAFETY: same trusted module, exact independent extension signature.
        let managed = unsafe { load_managed(library) }?;
        // SAFETY: independently versioned table of the same trusted module.
        let reactive = unsafe { load_reactive(library, release) }?;
        // SAFETY: independent optional table from the same trusted module.
        let composition = unsafe { load_composition(library, release) }?;
        // SAFETY: independently versioned optional table of this trusted module.
        let resources = unsafe { resources::load(library) }?;
        if reactive.is_some() && managed.is_none() {
            return Err(Error::Load("Reactive extension requires Managed v1".into()));
        }
        Ok(Some(Self {
            resources,
            composition,
            reactive,
            managed,
            dispatch,
            dependencies,
            interfaces,
        }))
    }
}
impl Module {
    pub(crate) async fn execute_scoped(
        &self,
        op: Op,
        instance: u64,
        method: &str,
        args: &Payload,
        scope: &Scope,
    ) -> Result<Reply> {
        let Some(extension) = &self.context else {
            return self
                .execute(
                    op,
                    instance,
                    method,
                    args,
                    scope.router.capabilities.validation,
                )
                .await;
        };
        let (operation, target) = match op {
            Op::Activate => (1, 0),
            Op::Invoke => (2, 0),
            Op::Deactivate => (3, 0),
            Op::Acquire => (4, 0),
            Op::ResourceCall(id) => (5, id),
            Op::ResourceRelease(id) => (6, id),
            Op::EffectRelease(id) => (7, id),
            Op::Managed(operation, id) => (operation, id),
            Op::Create | Op::Destroy => {
                return Err(Error::InvalidInput("not a scoped ABI operation".into()));
            }
        };
        if method.len() > cordis_wire::MAX_MESSAGE {
            return Err(Error::InvalidInput("method too long".into()));
        }
        let receive = {
            let mut buffer = Input::default();
            let value = buffer.build(args)?;
            let context = RawContext {
                host: scope.router.id,
                grant: scope.grant,
                api: &HOST,
            };
            // SAFETY: borrowed buffers live through dispatch. Scope registers tokens,
            // self pins guest code, HOST is static. No Context pointer is exposed.
            submit(
                self.release,
                matches!(op, Op::Acquire),
                scope.router.capabilities.validation,
                |completion| unsafe {
                    if let Some(dispatch) = extension.resources {
                        resources::dispatch(
                            extension,
                            dispatch,
                            instance,
                            operation,
                            target,
                            context,
                            Slice::new(method),
                            &value,
                            completion,
                        )
                    } else if let Some(composition) = &extension.composition {
                        (composition.dispatch)(
                            instance,
                            operation,
                            target,
                            CompositionContext {
                                base: context,
                                managed: if extension.managed.is_some() {
                                    &MANAGED_HOST
                                } else {
                                    ptr::null()
                                },
                                reactive: if extension.reactive.is_some() {
                                    &REACTIVE_HOST
                                } else {
                                    ptr::null()
                                },
                                composition: &COMPOSITION_HOST,
                            },
                            Slice::new(method),
                            &value,
                            completion,
                        )
                    } else if let Some(reactive) = &extension.reactive {
                        (reactive.dispatch)(
                            instance,
                            operation,
                            target,
                            ReactiveContext {
                                base: ManagedContext {
                                    base: context,
                                    managed: &MANAGED_HOST,
                                },
                                reactive: &REACTIVE_HOST,
                            },
                            Slice::new(method),
                            &value,
                            completion,
                        )
                    } else if let Some(dispatch) = extension.managed {
                        dispatch(
                            instance,
                            operation,
                            target,
                            ManagedContext {
                                base: context,
                                managed: &MANAGED_HOST,
                            },
                            Slice::new(method),
                            &value,
                            completion,
                        )
                    } else {
                        if matches!(op, Op::Managed(_, _)) {
                            return Err(Error::InvalidInput(
                                "Managed extension unavailable".into(),
                            ));
                        }
                        (extension.dispatch)(
                            instance,
                            operation,
                            target,
                            context,
                            Slice::new(method),
                            &value,
                            completion,
                        )
                    };
                    Ok(())
                },
            )?
        };
        receive
            .await
            .map_err(|_| Error::Unknown("ABI completion abandoned".into()))?
    }
}
static HOST: HostApi = HostApi {
    version: 1,
    size: mem::size_of::<HostApi>() as u32,
    invoke,
    defer,
    dispose,
    release,
};

// Buffers stay owned until the guest returns the result to release_result.
struct Owned {
    bytes: Bytes,
    code: String,
    message: String,
}
fn output(result: Result<Reply>) -> RawResult {
    let (status, bytes, handle, code, message) = match result {
        Ok(reply) => match reply.value.encode() {
            Ok(bytes) => (0, bytes, reply.handle, String::new(), String::new()),
            Err(e) => return output(Err(Error::Unknown(e.to_string()))),
        },
        Err(Error::Unknown(message)) => (2, Bytes::new(), 0, "unknown".into(), message),
        Err(Error::Rejected { code, message }) => (1, Bytes::new(), 0, code, message),
        Err(error) => {
            let (code, message) = match error {
                Error::Refused { code, message } => (code, message),
                Error::Closed => ("closed".into(), "Context or effect expired".into()),
                Error::Overloaded => ("overloaded".into(), "Context capacity reached".into()),
                other => ("host_error".into(), other.to_string()),
            };
            (3, Bytes::new(), 0, code, message)
        }
    };
    if code.len() + message.len() > cordis_wire::MAX_MESSAGE / 2 {
        return output(Err(Error::Unknown("invalid host result".into())));
    }
    let mut owner = Box::new(Owned {
        bytes,
        code,
        message,
    });
    let raw = RawResult {
        status,
        value: RawBuffer {
            data: owner.bytes.as_ptr(),
            len: owner.bytes.len(),
        },
        code: Slice::new(&owner.code),
        message: Slice::new(&owner.message),
        handle,
        owner: (&mut *owner as *mut Owned).cast(),
        ..RawResult::default()
    };
    let _ = Box::into_raw(owner);
    raw
}
fn protect(call: impl FnOnce() -> Result<Reply>) -> RawResult {
    catch_unwind(AssertUnwindSafe(|| output(call()))).unwrap_or_else(|_| RawResult {
        status: 2,
        ..RawResult::default()
    })
}
unsafe extern "C" fn release(raw: *mut RawResult) {
    // SAFETY: guest must return this exact result once, without changing owner.
    unsafe {
        if !(*raw).owner.is_null() {
            drop(Box::from_raw((*raw).owner.cast::<Owned>()));
        }
        *raw = RawResult::default();
    }
}
// Copy borrowed inputs and acquire the caller lease before returning to the
// plugin. The spawned operation remains owned even if its waiter goes away.
fn launch<F, Fut>(host: u64, caller: u64, completion: Completion, prepare: F)
where
    F: FnOnce(std::sync::Arc<Router>) -> Result<Fut>,
    Fut: std::future::Future<Output = Result<Reply>> + Send + 'static,
{
    let prepared = catch_unwind(AssertUnwindSafe(|| {
        let router = Router::get(host)?;
        let lease = router.admit(caller)?;
        let execution = lease.execution();
        let runtime = router.runtime.clone();
        let future = prepare(router)?;
        Ok((runtime, lease, execution, future))
    }))
    .unwrap_or_else(|_| Err(Error::Unknown("host submission panicked".into())));
    match prepared {
        Err(error) => {
            let raw = protect(|| Err(error));
            // SAFETY: every submission transfers one completion to the host.
            unsafe {
                completion.deliver(raw);
            }
        }
        Ok((runtime, lease, execution, future)) => {
            runtime.spawn(async move {
                use futures_util::FutureExt;
                let result = AssertUnwindSafe(crate::context::EXECUTION.scope(execution, future))
                    .catch_unwind()
                    .await
                    .unwrap_or_else(|_| Err(Error::Unknown("host operation panicked".into())));
                let raw = protect(|| result);
                // SAFETY: lease pins the plugin until its result consumer returns.
                unsafe {
                    completion.deliver(raw);
                }
                drop(lease);
            });
        }
    }
}
unsafe extern "C" fn invoke(
    host: u64,
    grant: u64,
    dependency: Slice,
    method: Slice,
    args: *const RawBuffer,
    caller: u64,
    completion: Completion,
) {
    launch(host, caller, completion, |router| {
        let mut budget = Budget::new();
        // SAFETY: guest supplies valid borrowed slices/buffer for this call.
        let (dependency, method, args) = unsafe {
            let args = read_slice(args, 1)?.first().ok_or_else(invalid_value)?;
            (
                read_text(dependency, &mut budget)?,
                read_text(method, &mut budget)?,
                read_payload(args, &mut budget, router.capabilities.validation)?,
            )
        };
        Ok(async move {
            let value = router
                .invoke(
                    grant,
                    dependency,
                    method,
                    args.one().map_err(|e| Error::InvalidInput(e.to_string()))?,
                )
                .await?;
            Ok(Reply {
                value: Payload::message(value),
                handle: 0,
            })
        })
    });
}
unsafe extern "C" fn defer(
    host: u64,
    grant: u64,
    callback: u64,
    label: Slice,
    caller: u64,
    completion: Completion,
) {
    launch(host, caller, completion, |router| {
        // SAFETY: guest supplies a readable label for this call.
        let label = unsafe { read_text(label, &mut Budget::new()) }?;
        Ok(async move {
            let handle = router.defer(grant, callback, label)?;
            Ok(Reply {
                value: Value::Null.into(),
                handle,
            })
        })
    });
}
unsafe extern "C" fn dispose(host: u64, effect: u64, caller: u64, completion: Completion) {
    launch(host, caller, completion, |router| {
        Ok(async move {
            // Cleanup cannot await its own recovery or form cleanup dependency cycles.
            if crate::context::in_cleanup() {
                return Err(Error::Refused {
                    code: "cleanup_dispose".into(),
                    message: "effect disposal during cleanup is forbidden".into(),
                });
            }
            router.dispose(effect).await?;
            Ok(Reply {
                value: Value::Null.into(),
                handle: 0,
            })
        })
    });
}

#[repr(C)]
struct ManagedHostApi {
    version: u32,
    size: u32,
    call: unsafe extern "C" fn(u64, u64, u32, u64, u64, Slice, *const RawBuffer, u64, Completion),
}
#[repr(C)]
struct ManagedContext {
    base: RawContext,
    managed: *const ManagedHostApi,
}
type ManagedDispatch =
    unsafe extern "C" fn(u64, u32, u64, ManagedContext, Slice, *const RawBuffer, Completion);
#[repr(C)]
#[derive(Default)]
struct ManagedApi {
    version: u32,
    size: u32,
    dispatch: Option<ManagedDispatch>,
}
unsafe fn load_managed(library: &libloading::Library) -> Result<Option<ManagedDispatch>> {
    type Entry = unsafe extern "C" fn(u32, *mut ManagedApi, usize) -> u32;
    // SAFETY: load accepts the optional native symbol's complete signature.
    let Ok(entry) = (unsafe { library.get::<Entry>(b"cordis_abi_managed_v1\0") }) else {
        return Ok(None);
    };
    let mut api = ManagedApi::default();
    // SAFETY: writable initialized exact-size table, owned until return.
    let status = unsafe { entry(1, &mut api, mem::size_of::<ManagedApi>()) };
    if status != 0
        || api.version != 1
        || api.size as usize != mem::size_of::<ManagedApi>()
        || api.dispatch.is_none()
    {
        return Err(Error::Load(
            "unsupported or incomplete ABI Managed v1 table".into(),
        ));
    }
    Ok(api.dispatch)
}
impl Module {
    pub fn managed(&self) -> bool {
        self.context
            .as_ref()
            .is_some_and(|ext| ext.managed.is_some())
    }
}
static MANAGED_HOST: ManagedHostApi = ManagedHostApi {
    version: 1,
    size: mem::size_of::<ManagedHostApi>() as u32,
    call: managed_call,
};
unsafe extern "C" fn managed_call(
    host: u64,
    grant: u64,
    operation: u32,
    target: u64,
    callback: u64,
    label: Slice,
    args: *const RawBuffer,
    caller: u64,
    completion: Completion,
) {
    launch(host, caller, completion, |router| {
        let mut budget = Budget::new();
        // SAFETY: borrowed input allocations remain readable through this call.
        let (label, args) = unsafe {
            let args = read_slice(args, 1)?.first().ok_or_else(invalid_value)?;
            (
                read_text(label, &mut budget)?,
                read_payload(args, &mut budget, router.capabilities.validation)?,
            )
        };
        Ok(async move {
            router
                .manage(
                    grant,
                    operation,
                    target,
                    callback,
                    label,
                    args.control_only()
                        .map_err(|e| Error::InvalidInput(e.to_string()))?,
                )
                .await
        })
    });
}

#[repr(C)]
struct ReactiveHostApi {
    version: u32,
    size: u32,
    call: unsafe extern "C" fn(u64, u64, u32, u64, u64, Slice, *const RawBuffer, u64, Completion),
}
#[repr(C)]
struct ReactiveContext {
    base: ManagedContext,
    reactive: *const ReactiveHostApi,
}
type ReactiveDispatch =
    unsafe extern "C" fn(u64, u32, u64, ReactiveContext, Slice, *const RawBuffer, Completion);
#[repr(C)]
#[derive(Default)]
struct ReactiveApi {
    version: u32,
    size: u32,
    describe: Option<unsafe extern "C" fn() -> RawResult>,
    dispatch: Option<ReactiveDispatch>,
}
struct ReactiveExtension {
    dispatch: ReactiveDispatch,
    manifest: Value,
}
unsafe fn load_reactive(
    library: &libloading::Library,
    release: Release,
) -> Result<Option<ReactiveExtension>> {
    type Entry = unsafe extern "C" fn(u32, *mut ReactiveApi, usize) -> u32;
    // SAFETY: the load caller trusts this optional native symbol signature.
    let Ok(entry) = (unsafe { library.get::<Entry>(b"cordis_abi_reactive_v1\0") }) else {
        return Ok(None);
    };
    let mut api = ReactiveApi::default();
    // SAFETY: initialized, writable exact-size table.
    let status = unsafe { entry(1, &mut api, mem::size_of::<ReactiveApi>()) };
    let invalid = || Error::Load("unsupported or incomplete ABI Reactive v1 table".into());
    if status != 0 || api.version != 1 || api.size as usize != mem::size_of::<ReactiveApi>() {
        return Err(invalid());
    }
    let describe = api.describe.ok_or_else(invalid)?;
    let dispatch = api.dispatch.ok_or_else(invalid)?;
    // SAFETY: describe obeys base result ownership, including error results.
    let manifest = finish(unsafe { describe() }, release, false, Validation::Audit)?
        .value
        .control_only()
        .map_err(|e| Error::Load(e.to_string()))?;
    if !manifest.as_object().is_some_and(|o| o.len() == 2) {
        return Err(invalid());
    }
    for key in ["events", "middleware"] {
        if !manifest[key].as_object().is_some_and(|o| {
            o.iter().all(|(alias, value)| {
                !alias.is_empty()
                    && alias.len() <= 1024
                    && value.as_object().is_some_and(|v| v.len() == 2)
                    && ["name", "revision"]
                        .iter()
                        .all(|key| value[key].as_str().is_some_and(|s| !s.is_empty()))
            })
        }) {
            return Err(invalid());
        }
    }
    Ok(Some(ReactiveExtension { dispatch, manifest }))
}
impl Module {
    pub fn reactive_manifest(&self) -> Option<&Value> {
        self.context
            .as_ref()?
            .reactive
            .as_ref()
            .map(|e| &e.manifest)
    }
}
static REACTIVE_HOST: ReactiveHostApi = ReactiveHostApi {
    version: 1,
    size: mem::size_of::<ReactiveHostApi>() as u32,
    call: reactive_call,
};
unsafe extern "C" fn reactive_call(
    host: u64,
    grant: u64,
    operation: u32,
    target: u64,
    callback: u64,
    alias: Slice,
    args: *const RawBuffer,
    caller: u64,
    completion: Completion,
) {
    launch(host, caller, completion, |router| {
        let mut budget = Budget::new();
        // SAFETY: borrowed input buffer and alias remain valid through this call.
        let (alias, args) = unsafe {
            let args = read_slice(args, 1)?.first().ok_or_else(invalid_value)?;
            (
                read_text(alias, &mut budget)?,
                read_payload(args, &mut budget, router.capabilities.validation)?,
            )
        };
        Ok(async move {
            router
                .react(grant, operation, target, callback, alias, args)
                .await
        })
    });
}

#[repr(C)]
struct CompositionHostApi {
    version: u32,
    size: u32,
    call: unsafe extern "C" fn(u64, u64, u32, u64, Slice, *const RawBuffer, u64, Completion),
}
#[repr(C)]
struct CompositionContext {
    base: RawContext,
    managed: *const ManagedHostApi,
    reactive: *const ReactiveHostApi,
    composition: *const CompositionHostApi,
}
type CompositionDispatch =
    unsafe extern "C" fn(u64, u32, u64, CompositionContext, Slice, *const RawBuffer, Completion);
#[repr(C)]
#[derive(Default)]
struct CompositionApi {
    version: u32,
    size: u32,
    describe: Option<unsafe extern "C" fn() -> RawResult>,
    dispatch: Option<CompositionDispatch>,
}
struct CompositionExtension {
    dispatch: CompositionDispatch,
    manifest: Value,
}
unsafe fn load_composition(
    library: &libloading::Library,
    release: Release,
) -> Result<Option<CompositionExtension>> {
    type Entry = unsafe extern "C" fn(u32, *mut CompositionApi, usize) -> u32;
    // SAFETY: caller trusts the native optional symbol signature.
    let Ok(entry) = (unsafe { library.get::<Entry>(b"cordis_abi_composition_v1\0") }) else {
        return Ok(None);
    };
    let mut api = CompositionApi::default();
    // SAFETY: initialized writable exact-size table.
    let status = unsafe { entry(1, &mut api, mem::size_of::<CompositionApi>()) };
    let invalid = || Error::Load("unsupported or incomplete ABI Composition v1 table".into());
    if status != 0 || api.version != 1 || api.size as usize != mem::size_of::<CompositionApi>() {
        return Err(invalid());
    }
    let describe = api.describe.ok_or_else(invalid)?;
    let dispatch = api.dispatch.ok_or_else(invalid)?;
    // SAFETY: manifest uses base result ownership; finish releases on failure.
    let manifest = finish(unsafe { describe() }, release, false, Validation::Audit)?
        .value
        .control_only()
        .map_err(|e| Error::Load(e.to_string()))?;
    let mut names = std::collections::BTreeSet::new();
    if !manifest.as_object().is_some_and(|o| o.len() == 2)
        || !manifest["factories"].as_array().is_some_and(|a| {
            a.iter().all(|v| {
                v.as_str()
                    .is_some_and(|s| !s.is_empty() && s.len() <= 1024 && names.insert(s))
            })
        })
        || !manifest["provisions"].as_object().is_some_and(|o| {
            o.iter().all(|(alias, v)| {
                !alias.is_empty()
                    && alias.len() <= 1024
                    && v.as_object().is_some_and(|o| o.len() == 2)
                    && ["name", "revision"]
                        .iter()
                        .all(|k| v[k].as_str().is_some_and(|s| !s.is_empty()))
            })
        })
    {
        return Err(invalid());
    }
    Ok(Some(CompositionExtension { dispatch, manifest }))
}
impl Module {
    pub fn composition_manifest(&self) -> Option<&Value> {
        self.context
            .as_ref()?
            .composition
            .as_ref()
            .map(|e| &e.manifest)
    }
}
static COMPOSITION_HOST: CompositionHostApi = CompositionHostApi {
    version: 1,
    size: mem::size_of::<CompositionHostApi>() as u32,
    call: composition_call,
};
unsafe extern "C" fn composition_call(
    host: u64,
    grant: u64,
    operation: u32,
    target: u64,
    alias: Slice,
    args: *const RawBuffer,
    caller: u64,
    completion: Completion,
) {
    launch(host, caller, completion, |router| {
        let mut budget = Budget::new();
        // SAFETY: guest lends readable buffer and alias through this call.
        let (alias, args) = unsafe {
            let args = read_slice(args, 1)?.first().ok_or_else(invalid_value)?;
            (
                read_text(alias, &mut budget)?,
                read_payload(args, &mut budget, router.capabilities.validation)?,
            )
        };
        Ok(async move { router.compose(grant, operation, target, alias, args).await })
    });
}
