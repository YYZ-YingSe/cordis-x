// Keep a valid base table while varying only the optional extension handshake.
#define cordis_abi_context_v1 original_context_entry
#include "../../examples/abi/context.cpp"
#undef cordis_abi_context_v1
extern "C" CORDIS_ABI_EXPORT uint32_t
cordis_abi_context_v1(uint32_t version, CordisAbiContextApi *out, size_t size) {
  auto status = original_context_entry(version, out, size);
  if (!status) {
#if BAD_CONTEXT == 1
    out->version = 2;
#elif BAD_CONTEXT == 2
    out->struct_size = 0;
#elif BAD_CONTEXT == 3
    out->dispatch = nullptr;
#else
    out->describe = []() noexcept { return cordis::abi::detail::result(42); };
#endif
  }
  return status;
}
