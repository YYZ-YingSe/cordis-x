#ifndef CORDIS_ABI_H
#define CORDIS_ABI_H
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

#define CORDIS_ABI_VERSION 1u
#define CORDIS_ABI_RESOURCES 1u
#define CORDIS_ABI_OK 0u
#define CORDIS_ABI_REJECTED 1u
#define CORDIS_ABI_REFUSED 3u
#define CORDIS_ABI_UNKNOWN 2u
#if defined(_WIN32)
#define CORDIS_ABI_EXPORT __declspec(dllexport)
#else
#define CORDIS_ABI_EXPORT __attribute__((visibility("default")))
#endif

/* Native platform C calling convention; all reserved fields must be zero.
 * Borrowed inputs live only until the function returns. Result pointers live
 * until release_result, which is called once for EVERY returned result.
 * No exceptions/unwinding may cross any function in this interface.
 * Full contract: docs/protocol/abi-v1.md. */
typedef struct {
  const uint8_t *data;
  size_t len;
} CordisAbiSlice;

/* Serialized cordis.transport.Payload; business messages are opaque protobuf
 * bytes. Input is borrowed until submission returns and must be copied before
 * suspension. Successful results own this buffer until release_result. */
typedef struct {
  const uint8_t *data;
  size_t len;
} CordisAbiBuffer;

typedef struct {
  uint32_t status;
  uint32_t reserved;
  CordisAbiBuffer value;
  CordisAbiSlice code;
  CordisAbiSlice message;
  uint64_t handle;
  void *owner;
} CordisAbiResult;

/* Completion consumes user_data exactly once, including admission failures.
 * The callback takes ownership of the result and must call release_result once.
 * Completion may occur before submission returns or on another thread; callers
 * must register ownership before submitting and only enqueue business
 * resumption. create/describe are local synchronous work; destroy joins every
 * execution and completion callback for that instance before permitting code
 * unload. */
typedef struct {
  void *user_data;
  void (*complete)(void *user_data, CordisAbiResult result);
} CordisAbiCompletion;

typedef struct {
  uint32_t abi_version;
  uint32_t struct_size;
  uint64_t capabilities;
  CordisAbiSlice contract_name;
  CordisAbiSlice contract_revision;
  /* Payload.control = {"validation": "contract"|"checked"|"audit", "config": business config}; no business bodies. */
  CordisAbiResult (*create)(const CordisAbiBuffer *config);
  void (*activate)(uint64_t instance, CordisAbiCompletion completion);
  void (*invoke)(uint64_t instance, CordisAbiSlice method,
                 const CordisAbiBuffer *args, CordisAbiCompletion completion);
  void (*deactivate)(uint64_t instance, CordisAbiCompletion completion);
  CordisAbiResult (*destroy)(uint64_t instance);
  void (*release_result)(CordisAbiResult *result);
  void (*resource_acquire)(uint64_t instance, CordisAbiSlice kind,
                           const CordisAbiBuffer *args,
                           CordisAbiCompletion completion);
  void (*resource_call)(uint64_t instance, uint64_t resource,
                        CordisAbiSlice method, const CordisAbiBuffer *args,
                        CordisAbiCompletion completion);
  void (*resource_release)(uint64_t instance, uint64_t resource,
                           CordisAbiCompletion completion);
  /* Complete current interface as protobuf control metadata; no instance is
   * created. */
  CordisAbiResult (*describe)(void);
} CordisAbiApi;

/* Returns 0 on success. Reject unsupported version/size without writing out.
 * The entry itself creates no instance; contract strings remain module-owned.
 */
CORDIS_ABI_EXPORT uint32_t cordis_abi_v1(uint32_t version, CordisAbiApi *out,
                                         size_t out_size);

#ifdef __cplusplus
}
#endif
#endif
