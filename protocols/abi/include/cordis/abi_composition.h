#ifndef CORDIS_ABI_COMPOSITION_H
#define CORDIS_ABI_COMPOSITION_H
#include <cordis/abi_reactive.h>
#ifdef __cplusplus
extern "C" {
#endif
#define CORDIS_ABI_COMPOSITION_VERSION 1u
#define CORDIS_ABI_CHILD_INSTALL 1u
#define CORDIS_ABI_CHILD_READY 2u
#define CORDIS_ABI_CHILD_SETTLED 3u
#define CORDIS_ABI_CHILD_DISPOSE 4u
#define CORDIS_ABI_CHILD_STATUS 5u
#define CORDIS_ABI_PROVIDE 6u
typedef struct {
  uint32_t version;
  uint32_t struct_size;
  void (*call)(uint64_t host, uint64_t grant, uint32_t operation,
               uint64_t target, CordisAbiSlice alias,
               const CordisAbiBuffer *args, uint64_t caller,
               CordisAbiCompletion completion);
} CordisAbiCompositionHostApi;
/* Requires Context v1. Managed/Reactive are independently optional; pointers
 * are NULL unless the corresponding guest tables were negotiated. */
typedef struct {
  CordisAbiContext base;
  const CordisAbiManagedHostApi *managed;
  const CordisAbiReactiveHostApi *reactive;
  const CordisAbiCompositionHostApi *composition;
} CordisAbiCompositionContext;
typedef struct {
  uint32_t version;
  uint32_t struct_size;
  CordisAbiResult (*describe)(void);
  /* Forwards all operations of the independently negotiated extensions. */
  void (*dispatch)(uint64_t instance, uint32_t operation, uint64_t target,
                   CordisAbiCompositionContext context, CordisAbiSlice method,
                   const CordisAbiBuffer *args, CordisAbiCompletion completion);
} CordisAbiCompositionApi;
CORDIS_ABI_EXPORT uint32_t cordis_abi_composition_v1(
    uint32_t version, CordisAbiCompositionApi *out, size_t out_size);
#ifdef __cplusplus
}
#endif
#endif
