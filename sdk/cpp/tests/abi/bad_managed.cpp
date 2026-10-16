#if BAD_MANAGED == 4
#define cordis_abi_context_v1 unused_context_entry
#endif
#include "../../examples/abi/context.cpp"
#if BAD_MANAGED == 4
#undef cordis_abi_context_v1
#endif
extern "C" CORDIS_ABI_EXPORT uint32_t
cordis_abi_managed_v1(uint32_t version, CordisAbiManagedApi *out, size_t size) {
  auto status = ContextHost<Model>::managed_entry(version, out, size);
  if (!status) {
#if BAD_MANAGED == 1
    out->version = 2;
#elif BAD_MANAGED == 2
    out->struct_size = 0;
#elif BAD_MANAGED == 3
    out->dispatch = nullptr;
#endif
  }
  return status;
}
