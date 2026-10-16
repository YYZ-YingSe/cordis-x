//! Resource capability calls with asynchronous completion and live Context authority.
use super::*;

#[repr(C)]
struct ResourceHostApi {
    version: u32,
    size: u32,
    call: unsafe extern "C" fn(u64, u64, u32, u64, Slice, Slice, *const RawBuffer, u64, Completion),
}
#[repr(C)]
pub(super) struct ResourceContext {
    base: CompositionContext,
    resources: *const ResourceHostApi,
}
pub(super) type Dispatch =
    unsafe extern "C" fn(u64, u32, u64, ResourceContext, Slice, *const RawBuffer, Completion);
#[repr(C)]
#[derive(Default)]
struct ResourceApi {
    version: u32,
    size: u32,
    dispatch: Option<Dispatch>,
}
pub(super) unsafe fn load(library: &libloading::Library) -> Result<Option<Dispatch>> {
    type Entry = unsafe extern "C" fn(u32, *mut ResourceApi, usize) -> u32;
    // SAFETY: Library::load accepts the trusted native symbol's signature.
    let Ok(entry) = (unsafe { library.get::<Entry>(b"cordis_abi_resources_v1\0") }) else {
        return Ok(None);
    };
    let mut api = ResourceApi::default();
    // SAFETY: initialized, writable storage of the exact advertised size.
    let status = unsafe { entry(1, &mut api, mem::size_of::<ResourceApi>()) };
    if status != 0
        || api.version != 1
        || api.size as usize != mem::size_of::<ResourceApi>()
        || api.dispatch.is_none()
    {
        return Err(Error::Load(
            "unsupported or incomplete ABI Resources v1 table".into(),
        ));
    }
    Ok(api.dispatch)
}
impl Module {
    pub fn dependency_resources(&self) -> bool {
        self.context
            .as_ref()
            .is_some_and(|ext| ext.resources.is_some())
    }
}
#[allow(clippy::too_many_arguments)]
pub(super) unsafe fn dispatch(
    extension: &Extension,
    dispatch: Dispatch,
    instance: u64,
    operation: u32,
    target: u64,
    context: RawContext,
    method: Slice,
    args: *const RawBuffer,
    completion: Completion,
) {
    // SAFETY: execute_scoped pins all borrowed arguments and guest code; these
    // host tables are static, with pointers only for negotiated extensions.
    unsafe {
        dispatch(
            instance,
            operation,
            target,
            ResourceContext {
                base: CompositionContext {
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
                    composition: if extension.composition.is_some() {
                        &COMPOSITION_HOST
                    } else {
                        ptr::null()
                    },
                },
                resources: &RESOURCE_HOST,
            },
            method,
            args,
            completion,
        )
    }
}
static RESOURCE_HOST: ResourceHostApi = ResourceHostApi {
    version: 1,
    size: mem::size_of::<ResourceHostApi>() as u32,
    call,
};
unsafe extern "C" fn call(
    host: u64,
    grant: u64,
    operation: u32,
    target: u64,
    dependency: Slice,
    method: Slice,
    args: *const RawBuffer,
    caller: u64,
    completion: Completion,
) {
    launch(host, caller, completion, |router| {
        let mut budget = Budget::new();
        // SAFETY: guest lends a readable input buffer and text until return.
        let (dependency, method, args) = unsafe {
            let args = read_slice(args, 1)?.first().ok_or_else(invalid_value)?;
            (
                read_text(dependency, &mut budget)?,
                read_text(method, &mut budget)?,
                read_payload(args, &mut budget, router.capabilities.validation)?,
            )
        };
        Ok(async move {
            router
                .resource_operation(grant, operation, target, dependency, method, args)
                .await
        })
    });
}
