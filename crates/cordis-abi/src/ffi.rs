//! The only module that dereferences foreign memory or calls foreign functions.
#![allow(unsafe_code)]

mod context;

use crate::{Bytes, Error, Payload, Result, Value};
use cordis_contract::{ContractId, Validation};
use std::{ffi::c_void, mem, path::Path, slice, str};

#[repr(C)]
#[derive(Clone, Copy, Default)]
struct Slice {
    data: *const u8,
    len: usize,
}
impl Slice {
    fn new(value: &str) -> Self {
        Self {
            data: value.as_ptr(),
            len: value.len(),
        }
    }
}
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct RawBuffer {
    data: *const u8,
    len: usize,
}
#[repr(C)]
#[derive(Default)]
struct RawResult {
    status: u32,
    reserved: u32,
    value: RawBuffer,
    code: Slice,
    message: Slice,
    handle: u64,
    owner: *mut c_void,
}
type Unary = unsafe extern "C" fn(u64, Completion);
type Destroy = unsafe extern "C" fn(u64) -> RawResult;
type Invoke = unsafe extern "C" fn(u64, Slice, *const RawBuffer, Completion);
type Release = unsafe extern "C" fn(*mut RawResult);
#[repr(C)]
#[derive(Default)]
struct Api {
    version: u32,
    size: u32,
    capabilities: u64,
    name: Slice,
    revision: Slice,
    create: Option<unsafe extern "C" fn(*const RawBuffer) -> RawResult>,
    activate: Option<Unary>,
    invoke: Option<Invoke>,
    deactivate: Option<Unary>,
    destroy: Option<Destroy>,
    release: Option<Release>,
    acquire: Option<Invoke>,
    resource_call: Option<unsafe extern "C" fn(u64, u64, Slice, *const RawBuffer, Completion)>,
    resource_release: Option<unsafe extern "C" fn(u64, u64, Completion)>,
    describe: Option<unsafe extern "C" fn() -> RawResult>,
}

// Store function pointers, not borrowed table/contract pointers. Library owns code.
pub(crate) struct Module {
    // Rust drops fields in declaration order: unload before deleting the staged
    // file, including on Windows where mapped DLLs cannot be removed.
    _library: libloading::Library,
    #[cfg(feature = "loader")]
    pub(crate) artifact: Option<crate::artifact::StagedArtifact>,
    pub contract: ContractId,
    pub descriptor: cordis_contract::Descriptor,
    pub context: Option<context::Extension>,
    create: unsafe extern "C" fn(*const RawBuffer) -> RawResult,
    activate: Unary,
    invoke: Invoke,
    deactivate: Unary,
    destroy: Destroy,
    release: Release,
    acquire: Option<Invoke>,
    resource_call: Option<unsafe extern "C" fn(u64, u64, Slice, *const RawBuffer, Completion)>,
    resource_release: Option<unsafe extern "C" fn(u64, u64, Completion)>,
}

#[derive(Clone, Copy)]
pub(crate) enum Op {
    Create,
    Activate,
    Invoke,
    Deactivate,
    Destroy,
    Acquire,
    ResourceCall(u64),
    ResourceRelease(u64),
    EffectRelease(u64),
    Managed(u32, u64),
}
pub(crate) struct Reply<T = Payload> {
    pub value: T,
    pub handle: u64,
}

// Restrict Windows dependency lookup to the DLL directory and host defaults.
// Explicit private dependency handles are retained by the package adapter.
pub(crate) unsafe fn open_library(path: &Path) -> Result<libloading::Library> {
    #[cfg(windows)]
    {
        use libloading::os::windows::{
            LOAD_LIBRARY_SEARCH_DEFAULT_DIRS, LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR, Library,
        };
        // SAFETY: delegated native-code contract; path is selected by the host.
        unsafe {
            Library::load_with_flags(
                path,
                LOAD_LIBRARY_SEARCH_DLL_LOAD_DIR | LOAD_LIBRARY_SEARCH_DEFAULT_DIRS,
            )
        }
        .map(Into::into)
        .map_err(|e| Error::Load(e.to_string()))
    }
    #[cfg(not(windows))]
    {
        // SAFETY: delegated native-code contract.
        unsafe {
            libloading::os::unix::Library::open(
                Some(path),
                libloading::os::unix::RTLD_NOW | libloading::os::unix::RTLD_LOCAL,
            )
        }
        .map(Into::into)
        .map_err(|e| Error::Load(e.to_string()))
    }
}

impl Module {
    // Safety is delegated to the public Library::load contract.
    pub unsafe fn load(path: &Path) -> Result<Self> {
        // SAFETY: caller accepts loader constructors/destructors and symbol ABI.
        let library = unsafe { open_library(path) }?;
        type Entry = unsafe extern "C" fn(u32, *mut Api, usize) -> u32;
        let mut api = Api::default();
        // SAFETY: entry writes at most size bytes to our initialized table.
        let status = unsafe {
            let entry = library
                .get::<Entry>(b"cordis_abi_v1\0")
                .map_err(|e| Error::Load(e.to_string()))?;
            entry(1, &mut api, mem::size_of::<Api>())
        };
        let invalid = || Error::Load("unsupported or incomplete ABI v1 function table".into());
        if status != 0
            || api.version != 1
            || api.size as usize != mem::size_of::<Api>()
            || api.capabilities & !1 != 0
        {
            return Err(invalid());
        }
        let resources = api.capabilities & 1 != 0;
        if [
            api.acquire.is_some(),
            api.resource_call.is_some(),
            api.resource_release.is_some(),
        ]
        .iter()
        .any(|present| *present != resources)
        {
            return Err(invalid());
        }
        let mut budget = Budget::new();
        // SAFETY: module contract strings are readable for their advertised length.
        let (name, revision) = unsafe {
            (
                read_text(api.name, &mut budget)?,
                read_text(api.revision, &mut budget)?,
            )
        };
        if name.is_empty() || revision.is_empty() {
            return Err(invalid());
        }
        let release = api.release.ok_or_else(invalid)?;
        let describe = api.describe.ok_or_else(invalid)?;
        // SAFETY: descriptor uses the same owned result contract as other calls.
        let description = finish(unsafe { describe() }, release, false, Validation::Audit)?
            .value
            .control_only()
            .map_err(|e| Error::Load(e.to_string()))?;
        let descriptor =
            cordis_contract::portable::decode(&description).map_err(Error::Contract)?;
        // SAFETY: optional extension belongs to the same trusted module.
        let context =
            unsafe { context::Extension::load(&library, api.release.ok_or_else(invalid)?) }?;
        Ok(Self {
            context,
            descriptor,
            _library: library,
            #[cfg(feature = "loader")]
            artifact: None,
            contract: ContractId::new(name, revision),
            create: api.create.ok_or_else(invalid)?,
            activate: api.activate.ok_or_else(invalid)?,
            invoke: api.invoke.ok_or_else(invalid)?,
            deactivate: api.deactivate.ok_or_else(invalid)?,
            destroy: api.destroy.ok_or_else(invalid)?,
            release: api.release.ok_or_else(invalid)?,
            acquire: api.acquire,
            resource_call: api.resource_call,
            resource_release: api.resource_release,
        })
    }
    pub fn resources(&self) -> bool {
        self.acquire.is_some()
    }

    pub fn execute_local(
        &self,
        op: Op,
        instance: u64,
        args: &Payload,
        validation: Validation,
    ) -> Result<Reply> {
        let mut buffer = Input::default();
        let value = buffer.build(args)?;
        // SAFETY: construction copies inputs; destruction joins the instance's
        // executor and completion callbacks before returning. self pins code.
        let raw = unsafe {
            match op {
                Op::Create => (self.create)(&value),
                Op::Destroy => (self.destroy)(instance),
                _ => return Err(Error::InvalidInput("not a local ABI operation".into())),
            }
        };
        finish(raw, self.release, matches!(op, Op::Create), validation)
    }
    pub async fn execute(
        &self,
        op: Op,
        instance: u64,
        method: &str,
        args: &Payload,
        validation: Validation,
    ) -> Result<Reply> {
        if method.len() > cordis_wire::MAX_MESSAGE {
            return Err(Error::InvalidInput("method too long".into()));
        }
        let receive = {
            let mut buffer = Input::default();
            let value = buffer.build(args)?;
            let method = Slice::new(method);
            // SAFETY: the plugin copies all borrowed inputs before submission
            // returns. Completion owns its result, and self pins release code.
            submit(
                self.release,
                matches!(op, Op::Acquire),
                validation,
                |completion| unsafe {
                    match op {
                        Op::Activate => (self.activate)(instance, completion),
                        Op::Invoke => (self.invoke)(instance, method, &value, completion),
                        Op::Deactivate => (self.deactivate)(instance, completion),
                        Op::Acquire => {
                            self.acquire.ok_or_else(|| {
                                Error::InvalidInput("resources unsupported".into())
                            })?(instance, method, &value, completion)
                        }
                        Op::ResourceCall(id) => {
                            self.resource_call.ok_or_else(|| {
                                Error::InvalidInput("resources unsupported".into())
                            })?(instance, id, method, &value, completion)
                        }
                        Op::ResourceRelease(id) => {
                            self.resource_release.ok_or_else(|| {
                                Error::InvalidInput("resources unsupported".into())
                            })?(instance, id, completion)
                        }
                        _ => {
                            return Err(Error::InvalidInput(
                                "operation requires its own capability".into(),
                            ));
                        }
                    }
                    Ok(())
                },
            )?
        };
        receive
            .await
            .map_err(|_| Error::Unknown("ABI completion abandoned".into()))?
    }
}

#[repr(C)]
#[derive(Clone, Copy)]
struct Completion {
    user_data: *mut c_void,
    complete: unsafe extern "C" fn(*mut c_void, RawResult),
}
// SAFETY: opaque user_data belongs to a one-shot completion; it is never
// dereferenced except by its originating callback. No borrowed inputs escape.
unsafe impl Send for Completion {}
impl Completion {
    unsafe fn deliver(self, result: RawResult) {
        // SAFETY: submission transfers the exactly-once callback contract.
        unsafe { (self.complete)(self.user_data, result) }
    }
}
struct Pending {
    send: tokio::sync::oneshot::Sender<Result<Reply>>,
    release: Release,
    acquisition: bool,
    validation: Validation,
}
unsafe extern "C" fn completed(data: *mut c_void, raw: RawResult) {
    // SAFETY: submit allocated this record; the plugin must complete once.
    let pending = unsafe { Box::from_raw(data.cast::<Pending>()) };
    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        finish(
            raw,
            pending.release,
            pending.acquisition,
            pending.validation,
        )
    }))
    .unwrap_or_else(|_| Err(Error::Unknown("invalid ABI completion".into())));
    // Tokio wakes the receiver; it never runs business code in this C callback.
    let _ = pending.send.send(result);
}
// The closure may fail only before it submits to foreign code.
fn submit(
    release: Release,
    acquisition: bool,
    validation: Validation,
    call: impl FnOnce(Completion) -> Result<()>,
) -> Result<tokio::sync::oneshot::Receiver<Result<Reply>>> {
    let (send, receive) = tokio::sync::oneshot::channel();
    let data = Box::into_raw(Box::new(Pending {
        send,
        release,
        acquisition,
        validation,
    }));
    let completion = Completion {
        user_data: data.cast(),
        complete: completed,
    };
    if let Err(error) = call(completion) {
        // SAFETY: the closure rejected before transferring this record.
        unsafe {
            drop(Box::from_raw(data));
        }
        return Err(error);
    }
    Ok(receive)
}

fn finish(
    raw: RawResult,
    release: Release,
    acquisition: bool,
    validation: Validation,
) -> Result<Reply> {
    let guard = Output { raw, release };
    let raw = &guard.raw;
    let mut budget = Budget::new();
    if raw.reserved != 0 {
        return Err(invalid_value());
    }
    // SAFETY: borrowed output buffers remain readable until the release hook.
    let (code, message) = unsafe {
        (
            read_text(raw.code, &mut budget)?,
            read_text(raw.message, &mut budget)?,
        )
    };
    match raw.status {
        0 if code.is_empty() && message.is_empty() => {
            if acquisition != (raw.handle != 0) {
                return Err(invalid_value());
            }
            let value = unsafe { read_payload(&raw.value, &mut budget, validation)? };
            Ok(Reply {
                value,
                handle: raw.handle,
            })
        }
        3 if raw.handle == 0 && !code.is_empty() => Err(Error::Refused { code, message }),
        1 if raw.handle == 0 && !code.is_empty() => Err(Error::Rejected { code, message }),
        2 => Err(Error::Unknown(format!("{code}: {message}"))),
        _ => Err(invalid_value()),
    }
}

struct Output {
    raw: RawResult,
    release: Release,
}
impl Drop for Output {
    fn drop(&mut self) {
        // SAFETY: this guard is unique, module still loaded, called even on invalid results.
        unsafe { (self.release)(&mut self.raw) }
    }
}

#[derive(Default)]
struct Input {
    bytes: Bytes,
}
impl Input {
    fn build(&mut self, value: &Payload) -> Result<RawBuffer> {
        self.bytes = value
            .encode()
            .map_err(|e| Error::InvalidInput(e.to_string()))?;
        Ok(RawBuffer {
            data: self.bytes.as_ptr(),
            len: self.bytes.len(),
        })
    }
}
fn invalid_value() -> Error {
    Error::Unknown("invalid ABI result".into())
}
struct Budget(usize);
impl Budget {
    fn new() -> Self {
        Self(cordis_wire::MAX_MESSAGE)
    }
    fn take(&mut self, count: usize) -> Result<()> {
        self.0 = self.0.checked_sub(count).ok_or_else(invalid_value)?;
        Ok(())
    }
}
unsafe fn read_slice<'a, T>(data: *const T, len: usize) -> Result<&'a [T]> {
    if len == 0 {
        return Ok(&[]);
    }
    if data.is_null()
        || !(data as usize).is_multiple_of(mem::align_of::<T>())
        || len > (isize::MAX as usize) / mem::size_of::<T>()
    {
        return Err(invalid_value());
    }
    // SAFETY: caller's trusted library guarantees initialized, readable allocations
    // of this size for the result lifetime; bounds checks cannot prove provenance.
    Ok(unsafe { slice::from_raw_parts(data, len) })
}
unsafe fn read_text(raw: Slice, budget: &mut Budget) -> Result<String> {
    budget.take(raw.len)?;
    // SAFETY: delegated readable result slice contract.
    let bytes = unsafe { read_slice(raw.data, raw.len)? };
    str::from_utf8(bytes)
        .map(str::to_owned)
        .map_err(|_| invalid_value())
}
unsafe fn read_payload(
    raw: &RawBuffer,
    budget: &mut Budget,
    validation: Validation,
) -> Result<Payload> {
    budget.take(raw.len)?;
    // SAFETY: copy before releasing foreign storage. Parsed bytes share our owner.
    let bytes = Bytes::copy_from_slice(unsafe { read_slice(raw.data, raw.len)? });
    Payload::decode_with(bytes, validation).map_err(|e| Error::Unknown(e.to_string()))
}
pub(crate) fn validate(value: &Value) -> Result<()> {
    cordis_wire::validate(value, 0).map_err(|e| Error::InvalidInput(e.to_string()))
}
#[cfg(test)]
mod tests;
