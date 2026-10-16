#ifndef CORDIS_ABI_RESOURCES_H
#define CORDIS_ABI_RESOURCES_H
#include <cordis/abi_composition.h>
#ifdef __cplusplus
extern "C" {
#endif
#define CORDIS_ABI_RESOURCES_VERSION 1u
#define CORDIS_ABI_DEPENDENCY_ACQUIRE 1u
#define CORDIS_ABI_DEPENDENCY_RESOURCE_CALL 2u
#define CORDIS_ABI_DEPENDENCY_RESOURCE_DISPOSE 3u

/* Dependency resource operations; requires Context.
 * Every operation requires a live Context grant. Acquire uses target=0 and
 * dependency/method=declared dependency/resource kind. Call/dispose use a
 * host-issued resource target and empty dependency; dispose also has an empty
 * method and null args. Only acquire returns a nonzero result handle.
 * Results use context.base.base.api->release_result, including failures.
 */
typedef struct {
  uint32_t version;
  uint32_t struct_size;
  void (*call)(uint64_t host, uint64_t grant, uint32_t operation,
               uint64_t target, CordisAbiSlice dependency,
               CordisAbiSlice method, const CordisAbiBuffer *args,
               uint64_t caller, CordisAbiCompletion completion);
} CordisAbiResourceHostApi;
typedef struct {
  /* Managed, Reactive and Composition pointers are independently optional. */
  CordisAbiCompositionContext base;
  const CordisAbiResourceHostApi *resources;
} CordisAbiResourceContext;
typedef struct {
  uint32_t version;
  uint32_t struct_size;
  /* Forwards all callbacks of the independently negotiated extensions. */
  void (*dispatch)(uint64_t instance, uint32_t operation, uint64_t target,
                   CordisAbiResourceContext context, CordisAbiSlice method,
                   const CordisAbiBuffer *args, CordisAbiCompletion completion);
} CordisAbiResourceApi;
CORDIS_ABI_EXPORT uint32_t cordis_abi_resources_v1(uint32_t version,
                                                   CordisAbiResourceApi *out,
                                                   size_t out_size);
#ifdef __cplusplus
}
#endif
#endif
