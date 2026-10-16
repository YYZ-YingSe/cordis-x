#include <cordis/abi.h>
CORDIS_ABI_EXPORT uint32_t cordis_abi_v1(uint32_t version, CordisAbiApi *out,
                                         size_t size) {
  if (version != CORDIS_ABI_VERSION || size != sizeof(*out) || !out)
    return 1;
  CordisAbiApi api = {0};
  api.abi_version = BAD_VERSION;
  api.struct_size = sizeof(api);
  *out = api;
  return 0;
}
