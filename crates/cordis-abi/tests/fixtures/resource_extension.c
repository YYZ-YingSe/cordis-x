#include "message_schema.h"
#include <cordis/abi_resources.h>
#include <stdlib.h>
#include <string.h>

static CordisAbiResult ok(void) {
  CordisAbiResult r = {0};
  r.value=(CordisAbiBuffer){null_payload,sizeof(null_payload)};
  return r;
}
static CordisAbiResult destroy(uint64_t id) {
  (void)id;
  return ok();
}
static void unary(uint64_t id, CordisAbiCompletion done) {
  (void)id;
  done.complete(done.user_data, ok());
}
static CordisAbiResult create(const CordisAbiBuffer *args) {
  (void)args;
  CordisAbiResult r = ok();
  r.handle = 1;
  return r;
}
static void invoke(uint64_t id, CordisAbiSlice method,
                   const CordisAbiBuffer *args, CordisAbiCompletion done) {
  (void)id;
  (void)method;
  (void)args;
  done.complete(done.user_data, ok());
}
static void release(CordisAbiResult *r) { memset(r, 0, sizeof(*r)); }
static CordisAbiSlice text(const char *s) {
  CordisAbiSlice v = {(const uint8_t *)s, strlen(s)};
  return v;
}

static CordisAbiResult describe_interface(void) {
  CordisAbiResult r=ok();
  r.value=(CordisAbiBuffer){interface_payload,sizeof(interface_payload)};
  if(SCENARIO==8) r.value=(CordisAbiBuffer){NULL,1};
  return r;
}

uint32_t cordis_abi_v1(uint32_t v, CordisAbiApi *out, size_t size) {
  if (v != 1 || !out || size != sizeof(*out))
    return 1;
  CordisAbiApi api = {0};
  api.abi_version = 1;
  api.struct_size = sizeof(api);
  api.contract_name = text("test.resource-extension");
  api.contract_revision = text("1");
  api.create = create;
  api.activate = unary;
  api.invoke = invoke;
  api.deactivate = unary;
  api.destroy = destroy;
  api.release_result = release;
  api.describe = SCENARIO == 7 ? NULL : describe_interface;
  *out = api;
  return 0;
}
static CordisAbiResult describe(void) {
  CordisAbiResult r=ok();
  r.value=(CordisAbiBuffer){context_payload,sizeof(context_payload)};
  return r;
}
static void base_dispatch(uint64_t id, uint32_t op, uint64_t target,
                          CordisAbiContext ctx, CordisAbiSlice method,
                          const CordisAbiBuffer *args,
                          CordisAbiCompletion done) {
  (void)id;
  (void)op;
  (void)target;
  (void)ctx;
  (void)method;
  (void)args;
  done.complete(done.user_data, ok());
}
#if SCENARIO != 5
uint32_t cordis_abi_context_v1(uint32_t v, CordisAbiContextApi *out,
                               size_t size) {
  if (v != 1 || !out || size != sizeof(*out))
    return 1;
  CordisAbiContextApi api = {1, sizeof(api), describe, base_dispatch};
  *out = api;
  return 0;
}
#endif
static uint64_t retired_grant;
struct Probe {
  CordisAbiResourceContext context;
  CordisAbiCompletion done;
  int index, valid;
};
static void step(struct Probe *probe);
static void checked(void *data, CordisAbiResult result) {
  struct Probe *probe = data;
  probe->valid &= result.status == CORDIS_ABI_REFUSED && result.handle == 0;
  probe->context.base.base.api->release_result(&result);
  ++probe->index;
  step(probe);
}
static void step(struct Probe *probe) {
  if (probe->index == 5) {
    CordisAbiResult result = ok();
    result.value=probe->valid ? (CordisAbiBuffer){answer_payload,sizeof(answer_payload)} : (CordisAbiBuffer){failed_payload,sizeof(failed_payload)};
    CordisAbiCompletion done = probe->done;
    free(probe);
    done.complete(done.user_data, result);
    return;
  }
  int i = probe->index;
  CordisAbiResourceContext ctx = probe->context;
  uint64_t grant = i == 0 ? retired_grant : ctx.base.base.grant;
  uint32_t operation = i < 2 ? CORDIS_ABI_DEPENDENCY_ACQUIRE
                             : CORDIS_ABI_DEPENDENCY_RESOURCE_CALL;
  uint64_t resource = i < 2 ? 0 : UINT64_MAX;
  if (i == 3)
    grant = 0;
  if (i == 4)
    operation = 99;
  CordisAbiBuffer value={argument_payload,sizeof(argument_payload)};
  CordisAbiCompletion completion = {probe, checked};
  ctx.resources->call(ctx.base.base.host, grant, operation, resource,
                      text(i < 2 ? "undeclared" : ""), text("counter"), &value,
                      ctx.base.base.grant, completion);
}
static void resource_dispatch(uint64_t id, uint32_t op, uint64_t target,
                              CordisAbiResourceContext ctx,
                              CordisAbiSlice method, const CordisAbiBuffer *args,
                              CordisAbiCompletion done) {
  (void)id;
  (void)target;
  (void)method;
  (void)args;
  if (op == CORDIS_ABI_ACTIVATE)
    retired_grant = ctx.base.base.grant;
  if (op != CORDIS_ABI_INVOKE) {
    done.complete(done.user_data, ok());
    return;
  }
  struct Probe *probe = calloc(1, sizeof(*probe));
  if (!probe) {
    CordisAbiResult r = ok();
    r.status = CORDIS_ABI_UNKNOWN;
    r.value=(CordisAbiBuffer){0};
    done.complete(done.user_data, r);
    return;
  }
  probe->context = ctx;
  probe->done = done;
  probe->valid = ctx.resources && ctx.resources->version == 1 &&
                 ctx.resources->struct_size == sizeof(*ctx.resources) &&
                 !ctx.base.managed && !ctx.base.reactive &&
                 !ctx.base.composition;
  if (!probe->valid)
    probe->index = 5;
  step(probe);
}
#if SCENARIO != 0
uint32_t cordis_abi_resources_v1(uint32_t v, CordisAbiResourceApi *out,
                                 size_t size) {
  if (v != 1 || !out || size != sizeof(*out) || SCENARIO == 6)
    return 1;
  CordisAbiResourceApi api = {1, sizeof(api), resource_dispatch};
  if (SCENARIO == 2)
    api.version = 2;
  if (SCENARIO == 3)
    --api.struct_size;
  if (SCENARIO == 4)
    api.dispatch = 0;
  *out = api;
  return 0;
}
#endif
