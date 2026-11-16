#ifndef CORDIS_ABI_CONTEXT_H
#define CORDIS_ABI_CONTEXT_H
#include <cordis/abi.h>
#ifdef __cplusplus
extern "C" {
#endif

#define CORDIS_ABI_CONTEXT_VERSION 1u
#define CORDIS_ABI_ACTIVATE 1u
#define CORDIS_ABI_INVOKE 2u
#define CORDIS_ABI_DEACTIVATE 3u
#define CORDIS_ABI_ACQUIRE 4u
#define CORDIS_ABI_RESOURCE_CALL 5u
#define CORDIS_ABI_RESOURCE_RELEASE 6u
#define CORDIS_ABI_EFFECT_RELEASE 7u

/* Pure observations share one cancellable submission contract. Cancellation
 * withdraws only this observer. The original completion still occurs exactly
 * once, possibly racing with withdrawal; retain callback storage until it runs.
 * target identifies an existing handle. args is a duration for timer/cancel
 * observations (null means no deadline for cancellation), or query parameters. */
#define CORDIS_ABI_OBSERVE_DELAY 0u
#define CORDIS_ABI_OBSERVE_CANCELLATION 1u
#define CORDIS_ABI_OBSERVE_TASK_JOIN 2u
#define CORDIS_ABI_OBSERVE_SEQUENCE_READY 3u
#define CORDIS_ABI_OBSERVE_SCOPE_READY 4u
#define CORDIS_ABI_OBSERVE_CHILD_READY 5u
#define CORDIS_ABI_OBSERVE_CHILD_SETTLED 6u
#define CORDIS_ABI_OBSERVE_DISPATCH_JOIN 7u
#define CORDIS_ABI_OBSERVE_SEQUENCE_STATUS 8u
#define CORDIS_ABI_OBSERVE_SCOPE_STATUS 9u
#define CORDIS_ABI_OBSERVE_TASK_STATUS 10u
#define CORDIS_ABI_OBSERVE_CHILD_STATUS 11u
#define CORDIS_ABI_OBSERVE_DISPATCH_STATUS 12u
#define CORDIS_ABI_OBSERVE_METADATA 13u

/* Context capability table.
 * Host functions/result releaser remain valid for the host process lifetime.
 * IDs are host-issued, non-reused tokens, never pointers to a borrowed Context.
 * Context grant expires after dispatch completion and admitted host calls
 * drain. caller identifies the executing callback, independently of a handle's
 * grant. It is required even for independent-handle operations (grant may be
 * zero). Submissions copy inputs before returning and settle completion exactly
 * once. No function may unwind across FFI.
 */
typedef struct {
  uint32_t version;
  uint32_t struct_size;
  void (*invoke)(uint64_t host, uint64_t grant, CordisAbiSlice dependency,
                 CordisAbiSlice method, const CordisAbiBuffer *args,
                 uint64_t caller, CordisAbiCompletion completion);
  void (*defer)(uint64_t host, uint64_t grant, uint64_t callback,
                CordisAbiSlice label, uint64_t caller,
                CordisAbiCompletion completion);
  void (*dispose)(uint64_t host, uint64_t effect, uint64_t caller,
                  CordisAbiCompletion completion);
  void (*release_result)(CordisAbiResult *result);
  void (*observe)(uint64_t host, uint64_t grant, uint64_t id, uint32_t kind,
                  uint64_t target, const CordisAbiBuffer *args,
                  uint64_t caller, CordisAbiCompletion completion);
  void (*cancel_observation)(uint64_t host, uint64_t caller, uint64_t id);
} CordisAbiHostApi;

typedef struct {
  uint64_t host;
  uint64_t grant;
  const CordisAbiHostApi *api;
} CordisAbiContext;

typedef struct {
  uint32_t version;
  uint32_t struct_size;
  /* { dependencies: {name: revision}, interfaces: {name: Interface} };
     the result is released by the base API. */
  CordisAbiResult (*describe)(void);
  void (*dispatch)(uint64_t instance, uint32_t operation, uint64_t target,
                   CordisAbiContext context, CordisAbiSlice method,
                   const CordisAbiBuffer *args, CordisAbiCompletion completion);
} CordisAbiContextApi;

CORDIS_ABI_EXPORT uint32_t cordis_abi_context_v1(uint32_t version,
                                                 CordisAbiContextApi *out,
                                                 size_t out_size);
#ifdef __cplusplus
}
#endif
#endif
