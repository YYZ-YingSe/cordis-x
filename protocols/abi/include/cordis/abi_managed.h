#ifndef CORDIS_ABI_MANAGED_H
#define CORDIS_ABI_MANAGED_H
#include <cordis/abi_context.h>
#ifdef __cplusplus
extern "C" {
#endif
#define CORDIS_ABI_MANAGED_VERSION 1u
/* Managed guest callback operations. */
#define CORDIS_ABI_SEQUENCE_NEXT 8u
#define CORDIS_ABI_SEQUENCE_UNDO 9u
#define CORDIS_ABI_SEQUENCE_RELEASE 10u
#define CORDIS_ABI_SCOPE_CALL 11u
#define CORDIS_ABI_TASK_CALL 12u
/* Host management operations. Only registration returns a nonzero handle. */
#define CORDIS_ABI_SEQUENCE_REGISTER 1u
#define CORDIS_ABI_SEQUENCE_READY 2u
#define CORDIS_ABI_SEQUENCE_DISPOSE 3u
#define CORDIS_ABI_SEQUENCE_STATUS 4u
#define CORDIS_ABI_SCOPE_CREATE 5u
#define CORDIS_ABI_SCOPE_READY 6u
#define CORDIS_ABI_SCOPE_RUN 7u
#define CORDIS_ABI_SCOPE_DISPOSE 8u
#define CORDIS_ABI_SCOPE_STATUS 9u
#define CORDIS_ABI_TASK_SPAWN 10u
#define CORDIS_ABI_TASK_JOIN 11u
#define CORDIS_ABI_TASK_STATUS 12u
#define CORDIS_ABI_TASK_CANCELLED 13u
/* Read pinned use configuration: label=name, args=contract revision. */
#define CORDIS_ABI_CONTEXT_METADATA 14u
typedef struct {
  uint32_t version;
  uint32_t struct_size;
  void (*call)(uint64_t host, uint64_t grant, uint32_t operation,
               uint64_t target, uint64_t callback, CordisAbiSlice label,
               const CordisAbiBuffer *args, uint64_t caller,
               CordisAbiCompletion completion);
} CordisAbiManagedHostApi;
typedef struct {
  CordisAbiContext base;
  const CordisAbiManagedHostApi *managed;
} CordisAbiManagedContext;
typedef struct {
  uint32_t version;
  uint32_t struct_size;
  void (*dispatch)(uint64_t instance, uint32_t operation, uint64_t target,
                   CordisAbiManagedContext context, CordisAbiSlice method,
                   const CordisAbiBuffer *args, CordisAbiCompletion completion);
} CordisAbiManagedApi;
/* Managed capabilities require Context. */
CORDIS_ABI_EXPORT uint32_t cordis_abi_managed_v1(uint32_t version,
                                                 CordisAbiManagedApi *out,
                                                 size_t out_size);
#ifdef __cplusplus
}
#endif
#endif
