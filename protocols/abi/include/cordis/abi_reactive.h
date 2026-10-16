#ifndef CORDIS_ABI_REACTIVE_H
#define CORDIS_ABI_REACTIVE_H
#include <cordis/abi_managed.h>
#ifdef __cplusplus
extern "C" {
#endif
#define CORDIS_ABI_REACTIVE_VERSION 1u
/* Reactive guest callback operations. */
#define CORDIS_ABI_EVENT_CALL 13u
#define CORDIS_ABI_MIDDLEWARE_CALL 14u
#define CORDIS_ABI_TERMINAL_CALL 15u
#define CORDIS_ABI_CALLBACK_RELEASE 16u
/* Host calls; ON, USE and EMIT return nonzero handles. */
#define CORDIS_ABI_EVENT_ON 1u
#define CORDIS_ABI_MIDDLEWARE_USE 2u
#define CORDIS_ABI_SUBSCRIPTION_DISPOSE 3u
#define CORDIS_ABI_EVENT_EMIT 4u
#define CORDIS_ABI_DISPATCH_JOIN 5u
#define CORDIS_ABI_MIDDLEWARE_RUN 6u
#define CORDIS_ABI_NEXT_RUN 7u
#define CORDIS_ABI_DISPATCH_STATUS 8u
typedef struct {
  uint32_t version;
  uint32_t struct_size;
  void (*call)(uint64_t host, uint64_t grant, uint32_t operation,
               uint64_t target, uint64_t callback, CordisAbiSlice alias,
               const CordisAbiBuffer *args, uint64_t caller,
               CordisAbiCompletion completion);
} CordisAbiReactiveHostApi;
typedef struct {
  CordisAbiManagedContext base;
  const CordisAbiReactiveHostApi *reactive;
} CordisAbiReactiveContext;
typedef struct {
  uint32_t version;
  uint32_t struct_size;
  CordisAbiResult (*describe)(void);
  void (*dispatch)(uint64_t instance, uint32_t operation, uint64_t target,
                   CordisAbiReactiveContext context, CordisAbiSlice method,
                   const CordisAbiBuffer *args, CordisAbiCompletion completion);
} CordisAbiReactiveApi;
/* Requires both Context v1 and Managed v1. */
CORDIS_ABI_EXPORT uint32_t cordis_abi_reactive_v1(uint32_t version,
                                                  CordisAbiReactiveApi *out,
                                                  size_t out_size);
#ifdef __cplusplus
}
#endif
#endif
