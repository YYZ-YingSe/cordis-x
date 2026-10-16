#define cordis_abi_reactive_v1 unused_reactive_entry
#if BAD_REACTIVE == 4
#define cordis_abi_managed_v1 unused_managed_entry
#elif BAD_REACTIVE == 5
#define cordis_abi_context_v1 unused_context_entry
#endif
#include "reactive.cpp"
#undef cordis_abi_reactive_v1
extern "C" CORDIS_ABI_EXPORT uint32_t cordis_abi_reactive_v1(
    uint32_t version, CordisAbiReactiveApi *out, size_t size) {
  auto status = ContextHost<Model>::reactive_entry(version, out, size);
  if (!status) {
#if BAD_REACTIVE == 1
    out->version = 2;
#elif BAD_REACTIVE == 2
    out->struct_size = 0;
#elif BAD_REACTIVE == 3
    out->dispatch = nullptr;
#elif BAD_REACTIVE == 6
    out->describe = [] {
      return detail::result(
          Value{{"events", nullptr}, {"middleware", Value::object()}});
    };
#endif
  }
  return status;
}
