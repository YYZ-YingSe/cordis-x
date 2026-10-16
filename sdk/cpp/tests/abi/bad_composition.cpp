#define cordis_abi_composition_v1 unused_composition_entry
#if BAD_COMPOSITION == 4
#define cordis_abi_context_v1 unused_context_entry
#endif
#include "composition.cpp"
#undef cordis_abi_composition_v1
extern "C" CORDIS_ABI_EXPORT uint32_t cordis_abi_composition_v1(
    uint32_t v, CordisAbiCompositionApi *out, size_t size) {
  auto status = ContextHost<Model>::composition_entry(v, out, size);
  if (!status) {
#if BAD_COMPOSITION == 1
    out->version = 2;
#elif BAD_COMPOSITION == 2
    out->struct_size = 0;
#elif BAD_COMPOSITION == 3
    out->dispatch = nullptr;
#elif BAD_COMPOSITION == 5
    out->describe = [] {
      return detail::result(Value{{"factories", {"worker", "worker"}},
                                  {"provisions", Value::object()}});
    };
#elif BAD_COMPOSITION == 6
    out->describe = [] {
      return detail::result(
          Value{{"factories", Value::array()}, {"provisions", nullptr}});
    };
#elif BAD_COMPOSITION == 7
    out->describe = nullptr;
#endif
  }
  return status;
}
