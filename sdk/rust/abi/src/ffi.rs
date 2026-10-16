//! The only ABI adapter module that calls foreign functions.
#![allow(unsafe_code)]
use super::*;
use cordis_sdk_core::Validation;
use raw::*;
use std::{
    any::TypeId,
    mem,
    panic::AssertUnwindSafe,
    sync::{
        Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
};
use value::{Budget, Input, slice};
#[cfg(test)]
mod resource_tests;
type HostCall = unsafe extern "C" fn(
    u64,
    u64,
    u32,
    u64,
    u64,
    CordisAbiSlice,
    *const CordisAbiBuffer,
    u64,
    CordisAbiCompletion,
);
type CompositionCall = unsafe extern "C" fn(
    u64,
    u64,
    u32,
    u64,
    CordisAbiSlice,
    *const CordisAbiBuffer,
    u64,
    CordisAbiCompletion,
);
type ResourceCall = unsafe extern "C" fn(
    u64,
    u64,
    u32,
    u64,
    CordisAbiSlice,
    CordisAbiSlice,
    *const CordisAbiBuffer,
    u64,
    CordisAbiCompletion,
);
#[derive(Clone, Copy)]
pub(crate) struct Host {
    validation: Validation,
    pub host: u64,
    pub grant: u64,
    invoke: unsafe extern "C" fn(
        u64,
        u64,
        CordisAbiSlice,
        CordisAbiSlice,
        *const CordisAbiBuffer,
        u64,
        CordisAbiCompletion,
    ),
    defer: unsafe extern "C" fn(u64, u64, u64, CordisAbiSlice, u64, CordisAbiCompletion),
    dispose: unsafe extern "C" fn(u64, u64, u64, CordisAbiCompletion),
    release: unsafe extern "C" fn(*mut CordisAbiResult),
    managed: Option<HostCall>,
    reactive: Option<HostCall>,
    composition: Option<CompositionCall>,
    resources: Option<ResourceCall>,
}
#[derive(Clone)]
pub(crate) enum Call {
    Invoke(String, String, Payload),
    Defer(u64, String),
    Dispose(u64),
    Managed(u32, u64, u64, String, Value),
    Reactive(u32, u64, u64, String, Payload),
    Composition(u32, u64, String, Payload),
    Resource(u32, u64, String, String, Payload),
}
pub(crate) struct Reply {
    pub value: Payload,
    pub handle: u64,
}
impl Reply {
    pub fn value(value: impl Into<Payload>) -> Self {
        Self {
            value: value.into(),
            handle: 0,
        }
    }
}
impl Host {
    unsafe fn read(ctx: CordisAbiCompositionContext) -> Result<Self> {
        // SAFETY: the trusted host promises tables readable for their size.
        let api = unsafe { ctx.base.api.as_ref() }
            .ok_or_else(|| refused("invalid_context", "missing host table"))?;
        if api.version != 1
            || api.struct_size as usize != mem::size_of::<CordisAbiHostApi>()
            || ctx.base.host == 0
            || ctx.base.grant == 0
        {
            return Err(refused("invalid_context", "invalid host table"));
        }
        macro_rules! extension {
            ($ptr:expr,$ty:ty) => {{
                match unsafe { $ptr.as_ref() } {
                    None => None,
                    Some(table) => {
                        if table.version != 1 || table.struct_size as usize != mem::size_of::<$ty>()
                        {
                            return Err(refused("invalid_context", "invalid extension"));
                        }
                        Some(
                            table
                                .call
                                .ok_or_else(|| refused("invalid_context", "missing host call"))?,
                        )
                    }
                }
            }};
        }
        Ok(Self {
            validation: Validation::Checked,
            host: ctx.base.host,
            grant: ctx.base.grant,
            invoke: api
                .invoke
                .ok_or_else(|| refused("invalid_context", "missing invoke"))?,
            defer: api
                .defer
                .ok_or_else(|| refused("invalid_context", "missing defer"))?,
            dispose: api
                .dispose
                .ok_or_else(|| refused("invalid_context", "missing dispose"))?,
            release: api
                .release_result
                .ok_or_else(|| refused("invalid_context", "missing releaser"))?,
            managed: extension!(ctx.managed, CordisAbiManagedHostApi),
            reactive: extension!(ctx.reactive, CordisAbiReactiveHostApi),
            composition: extension!(ctx.composition, CordisAbiCompositionHostApi),
            resources: None,
        })
    }
    unsafe fn read_resources(ctx: CordisAbiResourceContext) -> Result<Self> {
        let mut host = unsafe { Self::read(ctx.base) }?;
        if let Some(table) = unsafe { ctx.resources.as_ref() } {
            if table.version != 1
                || table.struct_size as usize != mem::size_of::<CordisAbiResourceHostApi>()
            {
                return Err(refused("invalid_context", "invalid resource host table"));
            }
            host.resources = Some(
                table
                    .call
                    .ok_or_else(|| refused("invalid_context", "missing resource call"))?,
            );
        }
        Ok(host)
    }
    pub async fn call(self, call: Call) -> Result<Reply> {
        let caller = worker::CALLER
            .try_with(|(host, grant)| {
                if *host == self.host {
                    Ok(*grant)
                } else {
                    Err(refused("invalid_handle", "foreign ABI instance"))
                }
            })
            .map_err(|_| refused("closed", "ABI call requires an executing callback"))??;
        let acquisition = match &call {
            Call::Defer(..) => true,
            Call::Managed(op, ..) => matches!(op, 1 | 5 | 10),
            Call::Reactive(op, ..) => matches!(op, 1 | 2 | 4),
            Call::Composition(op, ..) | Call::Resource(op, ..) => *op == 1,
            _ => false,
        };
        let receive = {
            let args = match &call {
                Call::Invoke(_, _, value)
                | Call::Reactive(_, _, _, _, value)
                | Call::Composition(_, _, _, value)
                | Call::Resource(_, _, _, _, value) => value.clone(),
                Call::Managed(_, _, _, _, value) => value.clone().into(),
                _ => Value::Null.into(),
            };
            let mut buffer = Input::default();
            let raw = buffer.build(&args)?;
            // SAFETY: host copies borrowed input before return and completes once.
            submit_host(self.release, acquisition, self.validation, |done| unsafe {
                match call {
                    Call::Invoke(name, method, _) => (self.invoke)(
                        self.host,
                        self.grant,
                        slice(&name),
                        slice(&method),
                        &raw,
                        caller,
                        done,
                    ),
                    Call::Defer(callback, label) => {
                        (self.defer)(self.host, self.grant, callback, slice(&label), caller, done)
                    }
                    Call::Dispose(id) => (self.dispose)(self.host, id, caller, done),
                    Call::Managed(op, target, callback, label, _) => self
                        .managed
                        .ok_or_else(|| refused("unsupported", "Managed unavailable"))?(
                        self.host,
                        self.grant,
                        op,
                        target,
                        callback,
                        slice(&label),
                        &raw,
                        caller,
                        done,
                    ),
                    Call::Reactive(op, target, callback, label, _) => self
                        .reactive
                        .ok_or_else(|| refused("unsupported", "Reactive unavailable"))?(
                        self.host,
                        self.grant,
                        op,
                        target,
                        callback,
                        slice(&label),
                        &raw,
                        caller,
                        done,
                    ),
                    Call::Composition(op, target, label, _) => self
                        .composition
                        .ok_or_else(|| refused("unsupported", "Composition unavailable"))?(
                        self.host,
                        self.grant,
                        op,
                        target,
                        slice(&label),
                        &raw,
                        caller,
                        done,
                    ),
                    Call::Resource(op, target, dependency, method, _) => {
                        self.resources.ok_or_else(|| {
                            refused("unsupported_resource", "Resource host unavailable")
                        })?(
                            self.host,
                            self.grant,
                            op,
                            target,
                            slice(&dependency),
                            slice(&method),
                            &raw,
                            caller,
                            done,
                        )
                    }
                }
                Ok(())
            })?
        };
        receive
            .await
            .map_err(|_| unknown("host abandoned ABI completion"))?
    }
}
struct PendingHost {
    send: tokio::sync::oneshot::Sender<Result<Reply>>,
    release: unsafe extern "C" fn(*mut CordisAbiResult),
    acquisition: bool,
    validation: Validation,
}
unsafe extern "C" fn host_completed(data: *mut std::ffi::c_void, raw: CordisAbiResult) {
    // SAFETY: this submission record is consumed once by the host callback.
    let pending = unsafe { Box::from_raw(data.cast::<PendingHost>()) };
    let result = std::panic::catch_unwind(AssertUnwindSafe(|| {
        consume(
            raw,
            pending.release,
            pending.acquisition,
            pending.validation,
        )
    }))
    .unwrap_or_else(|_| Err(unknown("invalid host completion")));
    let _ = pending.send.send(result);
}
fn submit_host(
    release: unsafe extern "C" fn(*mut CordisAbiResult),
    acquisition: bool,
    validation: Validation,
    submit: impl FnOnce(CordisAbiCompletion) -> Result<()>,
) -> Result<tokio::sync::oneshot::Receiver<Result<Reply>>> {
    let (send, receive) = tokio::sync::oneshot::channel();
    let data = Box::into_raw(Box::new(PendingHost {
        send,
        release,
        acquisition,
        validation,
    }));
    let completion = CordisAbiCompletion {
        user_data: data.cast(),
        complete: Some(host_completed),
    };
    if let Err(error) = submit(completion) {
        // SAFETY: missing capabilities reject before foreign submission.
        unsafe {
            drop(Box::from_raw(data));
        }
        return Err(error);
    }
    Ok(receive)
}
fn consume(
    result: CordisAbiResult,
    release: unsafe extern "C" fn(*mut CordisAbiResult),
    acquisition: bool,
    validation: Validation,
) -> Result<Reply> {
    struct Release(CordisAbiResult, unsafe extern "C" fn(*mut CordisAbiResult));
    impl Drop for Release {
        fn drop(&mut self) {
            unsafe { (self.1)(&mut self.0) }
        }
    }
    let result = Release(result, release);
    let r = &result.0;
    if r.reserved != 0 {
        return Err(unknown("invalid result reserved bits"));
    }
    if (r.status == 0 && (r.code.len != 0 || r.message.len != 0))
        || (r.status != 0 && (r.handle != 0 || r.value.len != 0))
        || (matches!(r.status, 1 | 3) && r.code.len == 0)
    {
        return Err(unknown("invalid ABI outcome fields"));
    }
    let mut budget = Budget::new();
    // SAFETY: host owns readable result storage until its matching release.
    unsafe {
        match r.status {
            0 if (r.handle != 0) == acquisition => Ok(Reply {
                value: value::read_payload(&r.value, &mut budget, validation)?,
                handle: r.handle,
            }),
            3 if r.handle == 0 => Err(Failure::Refused(ErrorInfo {
                code: value::read_text(r.code, &mut budget)?,
                message: value::read_text(r.message, &mut budget)?,
            })),
            1 if r.handle == 0 => Err(Failure::Rejected(ErrorInfo {
                code: value::read_text(r.code, &mut budget)?,
                message: value::read_text(r.message, &mut budget)?,
            })),
            2 => Err(Failure::Unknown(ErrorInfo {
                code: value::read_text(r.code, &mut budget)?,
                message: value::read_text(r.message, &mut budget)?,
            })),
            _ => Err(unknown("invalid ABI host result")),
        }
    }
}

struct Owned {
    bytes: Bytes,
    code: String,
    message: String,
}
fn output(result: Result<Reply>) -> CordisAbiResult {
    let (status, bytes, handle, code, message) = match result {
        Ok(r) => match r.value.encode() {
            Ok(bytes) => (0, bytes, r.handle, String::new(), String::new()),
            Err(e) => return output(Err(unknown(e))),
        },
        Err(Failure::Refused(e)) => (3, Bytes::new(), 0, e.code, e.message),
        Err(Failure::Rejected(e)) => (1, Bytes::new(), 0, e.code, e.message),
        Err(Failure::Unknown(e)) => (2, Bytes::new(), 0, e.code, e.message),
    };
    if code.len() + message.len() > cordis_wire::MAX_MESSAGE / 2 {
        return output(Err(unknown("plugin returned oversized ABI error")));
    }
    let mut owner = Box::new(Owned {
        bytes,
        code,
        message,
    });
    let raw = CordisAbiResult {
        status,
        value: CordisAbiBuffer {
            data: owner.bytes.as_ptr(),
            len: owner.bytes.len(),
        },
        code: slice(&owner.code),
        message: slice(&owner.message),
        handle,
        owner: (&mut *owner as *mut Owned).cast(),
        ..Default::default()
    };
    let _ = Box::into_raw(owner);
    raw
}
fn protect(f: impl FnOnce() -> Result<Reply>) -> CordisAbiResult {
    output(
        std::panic::catch_unwind(AssertUnwindSafe(f))
            .unwrap_or_else(|_| Err(unknown("ABI adapter panicked"))),
    )
}
unsafe extern "C" fn release(raw: *mut CordisAbiResult) {
    // SAFETY: the caller returns exactly once a result allocated by output.
    if let Some(raw) = unsafe { raw.as_mut() } {
        if !raw.owner.is_null() {
            unsafe { drop(Box::from_raw(raw.owner.cast::<Owned>())) };
        }
        *raw = Default::default();
    }
}
unsafe fn input(raw: *const CordisAbiBuffer, validation: Validation) -> Result<Payload> {
    let raw = unsafe { raw.as_ref() }.ok_or_else(|| refused("invalid_value", "null input"))?;
    unsafe { value::read_payload(raw, &mut Budget::new(), validation) }
        .map_err(|e| refused("invalid_value", e))
}
fn instances() -> &'static Mutex<BTreeMap<u64, (TypeId, worker::Client)>> {
    static INSTANCES: OnceLock<Mutex<BTreeMap<u64, (TypeId, worker::Client)>>> = OnceLock::new();
    INSTANCES.get_or_init(Default::default)
}
fn client<P: Plugin>(id: u64) -> Result<worker::Client> {
    instances()
        .lock()
        .unwrap()
        .get(&id)
        .filter(|(kind, _)| *kind == TypeId::of::<P>())
        .map(|(_, c)| c.clone())
        .ok_or_else(|| refused("closed", "instance absent"))
}
unsafe extern "C" fn create<P: Plugin>(config: *const CordisAbiBuffer) -> CordisAbiResult {
    protect(|| {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        let config = unsafe { input(config, Validation::Audit) }?
            .control_only()
            .map_err(|e| refused("invalid_config", e))?;
        let id = NEXT
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |id| id.checked_add(1))
            .map_err(|_| refused("exhausted", "instance IDs exhausted"))?;
        let settings = cordis_wire::Initialization::decode(config)
            .map_err(|e| refused("invalid_config", e))?;
        let client = worker::Client::start::<P>(settings.config, settings.validation)?;
        instances()
            .lock()
            .unwrap()
            .insert(id, (TypeId::of::<P>(), client));
        Ok(Reply {
            value: Value::Null.into(),
            handle: id,
        })
    })
}
#[derive(Clone, Copy)]
struct Completion(CordisAbiCompletion);
// SAFETY: the C contract permits completing from any thread exactly once.
unsafe impl Send for Completion {}
impl Completion {
    fn finish(self, result: Result<Reply>) {
        let raw = protect(|| result);
        // SAFETY: the host supplies a live completion for every submission.
        unsafe {
            self.0.complete.expect("missing ABI completion")(self.0.user_data, raw);
        }
    }
}
unsafe extern "C" fn unsupported(_: u64, done: CordisAbiCompletion) {
    Completion(done).finish(Err(refused("unsupported", "Context capability required")));
}
unsafe extern "C" fn unsupported_invoke(
    _: u64,
    _: CordisAbiSlice,
    _: *const CordisAbiBuffer,
    done: CordisAbiCompletion,
) {
    Completion(done).finish(Err(refused("unsupported", "Context capability required")));
}
unsafe extern "C" fn unsupported_resource_call(
    _: u64,
    _: u64,
    _: CordisAbiSlice,
    _: *const CordisAbiBuffer,
    done: CordisAbiCompletion,
) {
    Completion(done).finish(Err(refused("unsupported", "Context capability required")));
}
unsafe extern "C" fn unsupported_resource_release(_: u64, _: u64, done: CordisAbiCompletion) {
    Completion(done).finish(Err(refused("unsupported", "Context capability required")));
}
unsafe extern "C" fn destroy<P: Plugin>(id: u64) -> CordisAbiResult {
    protect(|| {
        client::<P>(id)?.close()?;
        instances().lock().unwrap().remove(&id);
        Ok(Reply::value(Value::Null))
    })
}
unsafe extern "C" fn dispatch<P: Plugin>(
    id: u64,
    op: u32,
    target: u64,
    ctx: CordisAbiCompositionContext,
    method: CordisAbiSlice,
    args: *const CordisAbiBuffer,
    done: CordisAbiCompletion,
) {
    unsafe {
        dispatch_with::<P>(id, op, target, Host::read(ctx), method, args, done);
    }
}
unsafe fn dispatch_with<P: Plugin>(
    id: u64,
    op: u32,
    target: u64,
    host: Result<Host>,
    method: CordisAbiSlice,
    args: *const CordisAbiBuffer,
    done: CordisAbiCompletion,
) {
    let done = Completion(done);
    let prepared = std::panic::catch_unwind(AssertUnwindSafe(|| {
        let client = client::<P>(id)?;
        let mut host = host?;
        host.validation = client.validation;
        let method = unsafe { value::read_text(method, &mut Budget::new()) }?;
        let args = unsafe { input(args, client.validation) }?;
        Ok((client, host, method, args))
    }))
    .unwrap_or_else(|_| Err(unknown("ABI submission panicked")));
    let (client, host, method, args) = match prepared {
        Ok(prepared) => prepared,
        Err(error) => {
            done.finish(Err(error));
            return;
        }
    };
    if let Err(error) = client.dispatch(host, op, target, method, args, move |result| {
        done.finish(result.and_then(|value| {
            if op == 4 {
                let handle = value
                    .control_only()
                    .map_err(unknown)?
                    .as_u64()
                    .filter(|id| *id != 0)
                    .ok_or_else(|| unknown("invalid acquired resource"))?;
                Ok(Reply {
                    value: Value::Null.into(),
                    handle,
                })
            } else {
                Ok(Reply::value(value))
            }
        }));
    }) {
        done.finish(Err(error));
    }
}
unsafe extern "C" fn resources_dispatch<P: Plugin>(
    id: u64,
    op: u32,
    target: u64,
    ctx: CordisAbiResourceContext,
    method: CordisAbiSlice,
    args: *const CordisAbiBuffer,
    done: CordisAbiCompletion,
) {
    unsafe {
        dispatch_with::<P>(
            id,
            op,
            target,
            Host::read_resources(ctx),
            method,
            args,
            done,
        );
    }
}

unsafe extern "C" fn context_dispatch<P: Plugin>(
    id: u64,
    op: u32,
    target: u64,
    ctx: CordisAbiContext,
    method: CordisAbiSlice,
    args: *const CordisAbiBuffer,
    done: CordisAbiCompletion,
) {
    unsafe {
        dispatch::<P>(
            id,
            op,
            target,
            CordisAbiCompositionContext {
                base: ctx,
                ..Default::default()
            },
            method,
            args,
            done,
        )
    }
}
unsafe extern "C" fn managed_dispatch<P: Plugin>(
    id: u64,
    op: u32,
    target: u64,
    ctx: CordisAbiManagedContext,
    method: CordisAbiSlice,
    args: *const CordisAbiBuffer,
    done: CordisAbiCompletion,
) {
    unsafe {
        dispatch::<P>(
            id,
            op,
            target,
            CordisAbiCompositionContext {
                base: ctx.base,
                managed: ctx.managed,
                ..Default::default()
            },
            method,
            args,
            done,
        )
    }
}
unsafe extern "C" fn reactive_dispatch<P: Plugin>(
    id: u64,
    op: u32,
    target: u64,
    ctx: CordisAbiReactiveContext,
    method: CordisAbiSlice,
    args: *const CordisAbiBuffer,
    done: CordisAbiCompletion,
) {
    unsafe {
        dispatch::<P>(
            id,
            op,
            target,
            CordisAbiCompositionContext {
                base: ctx.base.base,
                managed: ctx.base.managed,
                reactive: ctx.reactive,
                ..Default::default()
            },
            method,
            args,
            done,
        )
    }
}
unsafe extern "C" fn describe_interface<P: Plugin>() -> CordisAbiResult {
    protect(|| {
        let descriptor = cordis_sdk_core::Descriptor::new(P::interface())
            .map_err(|e| refused("invalid_contract", e))?;
        Ok(Reply::value(cordis_sdk_core::contract::encode(&descriptor)))
    })
}
unsafe extern "C" fn describe<P: Plugin>() -> CordisAbiResult {
    protect(|| {
        let m = P::manifest();
        let deps: BTreeMap<_, _> = m
            .requires
            .iter()
            .map(|c| (c.name.clone(), c.revision.clone()))
            .collect();
        if deps.len() != m.requires.len() {
            return Err(refused("invalid_manifest", "duplicate dependency"));
        }
        let interfaces = P::dependency_interfaces();
        if deps.keys().ne(interfaces.keys()) {
            return Err(refused(
                "invalid_contract",
                "dependency interface identities differ",
            ));
        }
        for interface in interfaces.values() {
            cordis_sdk_core::Descriptor::new(interface.clone())
                .map_err(|e| refused("invalid_contract", e))?;
        }
        Ok(Reply::value(
            json!({"dependencies":deps,"interfaces":interfaces}),
        ))
    })
}
fn contracts(map: BTreeMap<String, cordis_sdk_core::Contract>) -> Value {
    json!(
        map.into_iter()
            .map(|(alias, c)| (alias, json!({"name":c.name,"revision":c.revision})))
            .collect::<BTreeMap<_, _>>()
    )
}
unsafe extern "C" fn describe_reactive<P: Plugin>() -> CordisAbiResult {
    protect(|| {
        let b = P::bindings();
        Ok(Reply::value(
            json!({"events":contracts(b.events),"middleware":contracts(b.middleware)}),
        ))
    })
}
unsafe extern "C" fn describe_composition<P: Plugin>() -> CordisAbiResult {
    protect(|| {
        let mut b = P::bindings();
        b.factories.sort();
        Ok(Reply::value(
            json!({"factories":b.factories,"provisions":contracts(b.provisions)}),
        ))
    })
}
unsafe fn table<T>(v: u32, out: *mut T, size: usize, create: impl FnOnce() -> T) -> u32 {
    if v != 1 || out.is_null() || size != mem::size_of::<T>() {
        return 1;
    }
    std::panic::catch_unwind(AssertUnwindSafe(|| unsafe { out.write(create()) })).map_or(1, |_| 0)
}
/// # Safety
/// `out` must designate writable, aligned storage of the advertised table size.
pub unsafe fn entry<P: Plugin>(v: u32, out: *mut CordisAbiApi, size: usize) -> u32 {
    unsafe {
        table(v, out, size, || {
            static MANIFESTS: OnceLock<
                Mutex<BTreeMap<TypeId, &'static cordis_sdk_core::PluginManifest>>,
            > = OnceLock::new();
            let mut manifests = MANIFESTS.get_or_init(Default::default).lock().unwrap();
            let manifest = *manifests
                .entry(TypeId::of::<P>())
                .or_insert_with(|| Box::leak(Box::new(P::manifest())));
            CordisAbiApi {
                abi_version: 1,
                struct_size: mem::size_of::<CordisAbiApi>() as u32,
                capabilities: CORDIS_ABI_RESOURCES as u64,
                resource_acquire: Some(unsupported_invoke),
                resource_call: Some(unsupported_resource_call),
                resource_release: Some(unsupported_resource_release),
                contract_name: slice(&manifest.provides.name),
                contract_revision: slice(&manifest.provides.revision),
                create: Some(create::<P>),
                activate: Some(unsupported),
                invoke: Some(unsupported_invoke),
                deactivate: Some(unsupported),
                destroy: Some(destroy::<P>),
                release_result: Some(release),
                describe: Some(describe_interface::<P>),
            }
        })
    }
}
/// # Safety
/// Same output storage requirements as [`entry`].
pub unsafe fn context_entry<P: Plugin>(v: u32, out: *mut CordisAbiContextApi, size: usize) -> u32 {
    unsafe {
        table(v, out, size, || CordisAbiContextApi {
            version: 1,
            struct_size: mem::size_of::<CordisAbiContextApi>() as u32,
            describe: Some(describe::<P>),
            dispatch: Some(context_dispatch::<P>),
        })
    }
}
/// # Safety
/// Same output storage requirements as [`entry`].
pub unsafe fn managed_entry<P: Plugin>(v: u32, out: *mut CordisAbiManagedApi, size: usize) -> u32 {
    unsafe {
        table(v, out, size, || CordisAbiManagedApi {
            version: 1,
            struct_size: mem::size_of::<CordisAbiManagedApi>() as u32,
            dispatch: Some(managed_dispatch::<P>),
        })
    }
}
/// # Safety
/// Same output storage requirements as [`entry`].
pub unsafe fn reactive_entry<P: Plugin>(
    v: u32,
    out: *mut CordisAbiReactiveApi,
    size: usize,
) -> u32 {
    unsafe {
        table(v, out, size, || CordisAbiReactiveApi {
            version: 1,
            struct_size: mem::size_of::<CordisAbiReactiveApi>() as u32,
            describe: Some(describe_reactive::<P>),
            dispatch: Some(reactive_dispatch::<P>),
        })
    }
}
/// # Safety
/// Same output storage requirements as [`entry`].
pub unsafe fn composition_entry<P: Plugin>(
    v: u32,
    out: *mut CordisAbiCompositionApi,
    size: usize,
) -> u32 {
    unsafe {
        table(v, out, size, || CordisAbiCompositionApi {
            version: 1,
            struct_size: mem::size_of::<CordisAbiCompositionApi>() as u32,
            describe: Some(describe_composition::<P>),
            dispatch: Some(dispatch::<P>),
        })
    }
}

/// # Safety
/// Same output storage requirements as [`entry`].
pub unsafe fn resources_entry<P: Plugin>(
    v: u32,
    out: *mut CordisAbiResourceApi,
    size: usize,
) -> u32 {
    unsafe {
        table(v, out, size, || CordisAbiResourceApi {
            version: 1,
            struct_size: mem::size_of::<CordisAbiResourceApi>() as u32,
            dispatch: Some(resources_dispatch::<P>),
        })
    }
}
