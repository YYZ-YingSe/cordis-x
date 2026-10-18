#pragma once
#include <chrono>
#include <cordis/abi_composition.h>
#include <cordis/abi_context.h>
#include <cordis/abi_reactive.h>
#include <cordis/abi_resources.h>
#include <cordis/adapters/abi/abi.hpp>
#include <functional>

namespace cordis::abi {
using HostReply = std::pair<Payload, uint64_t>;
class Context;
class Sequence;
class Scope;
class Task;
struct Step;
namespace detail {
struct Callbacks {
  Validation validation = Validation::Checked;
  std::mutex mutex;
  uint64_t next = 1;
  using Control = std::function<Async<Value>(uint32_t, Context, Value)>;
  using Managed = std::function<Async<Payload>(uint32_t, Context, Payload)>;
  uint64_t insert_managed(Control call);
  std::unordered_map<uint64_t, std::shared_ptr<Managed>> managed;
  std::unordered_map<uint64_t, std::function<Async<void>(Context)>> entries;
  uint64_t insert(std::function<Async<void>(Context)> callback) {
    std::lock_guard<std::mutex> lock(mutex);
    if (next == std::numeric_limits<uint64_t>::max())
      throw Refusal("exhausted", "callback space exhausted");
    auto id = next++;
    entries.emplace(id, std::move(callback));
    return id;
  }
  std::function<Async<void>(Context)> get(uint64_t id) {
    std::lock_guard<std::mutex> lock(mutex);
    auto found = entries.find(id);
    if (found == entries.end())
      throw Refusal("closed", "unknown callback");
    return found->second;
  }
  void erase(uint64_t id) {
    // Destroy user captures outside the registry lock too.
    decltype(entries)::node_type removed;
    {
      std::lock_guard<std::mutex> lock(mutex);
      removed = entries.extract(id);
    }
  }
  uint64_t insert_payload(Managed call) {
    auto value = std::make_shared<Managed>(std::move(call));
    std::lock_guard<std::mutex> lock(mutex);
    if (next == std::numeric_limits<uint64_t>::max())
      throw Refusal("exhausted", "callback space exhausted");
    auto id = next++;
    managed.emplace(id, std::move(value));
    return id;
  }
  std::shared_ptr<Managed> get_managed(uint64_t id) {
    std::lock_guard<std::mutex> lock(mutex);
    auto found = managed.find(id);
    if (found == managed.end())
      throw Refusal("closed", "unknown managed callback");
    return found->second;
  }
  void erase_managed(uint64_t id) {
    decltype(managed)::node_type removed;
    {
      std::lock_guard<std::mutex> lock(mutex);
      removed = managed.extract(id);
    }
  }
  bool empty() {
    std::lock_guard<std::mutex> lock(mutex);
    return entries.empty() && managed.empty();
  }
};
inline HostReply consume(const CordisAbiHostApi *api, CordisAbiResult result,
                         bool acquisition = false, Validation validation = Validation::Checked) {
  struct Guard {
    const CordisAbiHostApi *api;
    CordisAbiResult &result;
    ~Guard() { api->release_result(&result); }
  } guard{api, result};
  auto code = text(result.code), message = text(result.message);
  if (result.reserved)
    throw std::runtime_error("invalid host result");
  if (result.status == CORDIS_ABI_REFUSED && !code.empty() && !result.handle)
    throw Refusal(code, message);
  if (result.status == CORDIS_ABI_REJECTED && !code.empty() && !result.handle)
    throw Rejected(code, message);
  if (result.status != CORDIS_ABI_OK || !code.empty() || !message.empty() ||
      acquisition != (result.handle != 0))
    throw std::runtime_error("unknown host outcome: " + code + ": " + message);
  return {read(&result.value, validation), result.handle};
}
template <class Submit>
Async<HostReply> receive(const CordisAbiHostApi *api, uint64_t host,
                         bool acquisition, Submit submit) {
  auto caller = co_await current_caller(host);
  co_return co_await native::completed<HostReply>([&](auto resume) {
    struct Pending {
      const CordisAbiHostApi *api;
      bool acquisition;
      Validation validation;
      std::function<void(native::Outcome<HostReply>)> resume;
      static void finish(void *data, CordisAbiResult result) noexcept {
        std::unique_ptr<Pending> pending(static_cast<Pending *>(data));
        native::Outcome<HostReply> outcome;
        try {
          outcome = consume(pending->api, result, pending->acquisition, pending->validation);
        } catch (...) {
          outcome = std::current_exception();
        }
        pending->resume(std::move(outcome));
      }
    };
    auto pending =
        std::make_unique<Pending>(Pending{api, acquisition, caller.validation, std::move(resume)});
    CordisAbiCompletion completion{pending.get(), Pending::finish};
    pending.release();
    submit(caller.grant, completion);
  });
}
} // namespace detail

// Copyable, independent of the originating Context grant. Explicit disposal
// shares the native Effect's cached outcome; destruction does not dispose it.
class Effect {
  uint64_t host_ = 0, id_ = 0;
  const CordisAbiHostApi *api_ = nullptr;
  friend class Context;
  class Sequence;
  class Scope;
  class Task;
  struct Step;
  Effect(uint64_t host, uint64_t id, const CordisAbiHostApi *api)
      : host_(host), id_(id), api_(api) {}

public:
  Effect() = default;
  Async<void> dispose() const {
    return cordis::detail::invoke_owned([*this]() -> Async<void> {
      if (!api_)
        throw Refusal("closed", "empty effect");
      (co_await detail::receive(
          api_, host_, false,
          [&](uint64_t caller, CordisAbiCompletion completion) {
            api_->dispose(host_, id_, caller, completion);
          }));

      co_return;
    });
  }
};

class Subscription;
class Dispatch;
class Next;
class Child;
class Context {
  CordisAbiContext raw_;
  const CordisAbiManagedHostApi *managed_ = nullptr;
  const CordisAbiReactiveHostApi *reactive_ = nullptr;
  const CordisAbiCompositionHostApi *composition_ = nullptr;
  const CordisAbiResourceHostApi *resources_ = nullptr;
  std::shared_ptr<detail::Callbacks> callbacks_;

public:
  Validation validation() const { return callbacks_->validation; }
  class ResourceHandle {
    uint64_t host, id;
    std::weak_ptr<detail::Callbacks> owner;
    ResourceHandle(uint64_t host, uint64_t id,
                   const std::shared_ptr<detail::Callbacks> &owner)
        : host(host), id(id), owner(owner) {}
    friend class Context;
  };
  Context(CordisAbiContext raw, std::shared_ptr<detail::Callbacks> callbacks,
          const CordisAbiManagedHostApi *managed = nullptr,
          const CordisAbiReactiveHostApi *reactive = nullptr,
          const CordisAbiCompositionHostApi *composition = nullptr,
          const CordisAbiResourceHostApi *resources = nullptr)
      : raw_(raw), managed_(managed), reactive_(reactive),
        composition_(composition), resources_(resources),
        callbacks_(std::move(callbacks)) {
    if (!raw.api || raw.api->version != CORDIS_ABI_CONTEXT_VERSION ||
        raw.api->struct_size != sizeof(CordisAbiHostApi) || !raw.host ||
        !raw.grant || !raw.api->invoke || !raw.api->defer ||
        !raw.api->dispose || !raw.api->release_result)
      throw Refusal("context", "unsupported host Context");
    if (managed && (managed->version != 1 ||
                    managed->struct_size != sizeof(CordisAbiManagedHostApi) ||
                    !managed->call))
      throw Refusal("context", "unsupported Managed host");
    if (reactive &&
        (reactive->version != 1 ||
         reactive->struct_size != sizeof(CordisAbiReactiveHostApi) ||
         !reactive->call))
      throw Refusal("context", "unsupported Reactive host");
    if (composition &&
        (composition->version != 1 ||
         composition->struct_size != sizeof(CordisAbiCompositionHostApi) ||
         !composition->call))
      throw Refusal("context", "unsupported Composition host");
    if (resources &&
        (resources->version != CORDIS_ABI_RESOURCES_VERSION ||
         resources->struct_size != sizeof(CordisAbiResourceHostApi) ||
         !resources->call))
      throw Refusal("context", "invalid Resource host");
  }
  Async<ResourceHandle> acquire(std::string dependency, std::string kind,
                                Message args) const {
    return cordis::detail::invoke_owned(
        [*this](std::string dependency, std::string kind,
                Message args) -> Async<ResourceHandle> {
          auto reply =
              (co_await resource_call(CORDIS_ABI_DEPENDENCY_ACQUIRE, 0,
                                      dependency, kind, Payload(args)));
          if (!reply.first.control_only().is_null())
            throw std::runtime_error(
                "invalid resource acquisition acknowledgement");
          co_return ResourceHandle(raw_.host, reply.second, callbacks_);
        },
        std::move(dependency), std::move(kind), std::move(args));
  }
  Async<Message> resource_invoke(ResourceHandle resource, std::string method,
                                 Message args) const {
    return cordis::detail::invoke_owned(
        [*this](ResourceHandle resource, std::string method,
                Message args) -> Async<Message> {
          check_resource(resource);
          co_return (co_await resource_call(CORDIS_ABI_DEPENDENCY_RESOURCE_CALL,
                                            resource.id, "", method,
                                            Payload(args)))
              .first.one();
        },
        std::move(resource), std::move(method), std::move(args));
  }
  Async<void> resource_dispose(ResourceHandle resource) const {
    return cordis::detail::invoke_owned(
        [*this](ResourceHandle resource) -> Async<void> {
          check_resource(resource);
          if (!(co_await resource_call(CORDIS_ABI_DEPENDENCY_RESOURCE_DISPOSE,
                                       resource.id, "", "", Payload()))
                   .first.control_only()
                   .is_null())
            throw std::runtime_error(
                "invalid resource disposal acknowledgement");

          co_return;
        },
        std::move(resource));
  }

private:
  void check_resource(ResourceHandle resource) const {
    if (resource.host != raw_.host || resource.owner.lock() != callbacks_)
      throw Refusal("invalid_handle", "resource belongs to another instance");
  }
  Async<HostReply> resource_call(uint32_t operation, uint64_t target,
                                 std::string dependency, std::string method,
                                 Payload args) const {
    return cordis::detail::invoke_owned(
        [*this](uint32_t operation, uint64_t target, std::string dependency,
                std::string method, Payload args) -> Async<HostReply> {
          if (!resources_)
            throw Refusal("context",
                          "resource operation requires Resource Context");
          detail::Output input;
          detail::Budget budget;
          const auto raw = input.build(args, budget);
          co_return (co_await detail::receive(
              raw_.api, raw_.host, operation == CORDIS_ABI_DEPENDENCY_ACQUIRE,
              [&](uint64_t caller, CordisAbiCompletion completion) {
                resources_->call(raw_.host, raw_.grant, operation, target,
                                 detail::slice(dependency),
                                 detail::slice(method), &raw, caller,
                                 completion);
              }));
        },
        std::move(operation), std::move(target), std::move(dependency),
        std::move(method), std::move(args));
  }

public:
  Async<Child> install(std::string factory, Message config) const;
  Async<void> provide(std::string alias, Message value) const;
  Async<Subscription> on(std::string alias,
                         std::function<Async<Flow>(Context, Message)> handler,
                         bool initializing = false) const;
  Async<Subscription> middleware(
      std::string alias,
      std::function<Async<Message>(Context, Message, Next)> handler) const;
  Async<Dispatch> emit(std::string alias, Message input,
                       bool serial = false) const;
  Async<std::vector<Flow>> parallel(std::string alias, Message input) const;
  Async<Flow> serial(std::string alias, Message input) const;
  Async<Message>
  run(std::string alias, Message input,
      std::function<Async<Message>(Context, Message)> terminal) const;
  Async<Sequence> effect(std::string label,
                         std::function<Async<Step>(Context)> next) const;
  Async<Scope> scope(std::string label) const;
  Async<Task> spawn(std::string label,
                    std::function<Async<void>(Context)> run) const;
  Async<void> cancelled() const;
  Async<Value> metadata(std::string name, std::string revision) const;
  Async<bool> cancelled_for(std::chrono::milliseconds duration) const;
  Async<Message> invoke(std::string dependency, std::string method,
                        Message args) const {
    return cordis::detail::invoke_owned(
        [*this](std::string dependency, std::string method,
                Message args) -> Async<Message> {
          detail::Output input;
          detail::Budget budget;
          auto raw = input.build(args, budget);
          co_return (co_await detail::receive(
                         raw_.api, raw_.host, false,
                         [&](uint64_t caller, CordisAbiCompletion completion) {
                           raw_.api->invoke(
                               raw_.host, raw_.grant, detail::slice(dependency),
                               detail::slice(method), &raw, caller, completion);
                         }))
              .first.one();
        },
        std::move(dependency), std::move(method), std::move(args));
  }
  Async<Effect> defer(std::string label,
                      std::function<Async<void>(Context)> cleanup) const {
    return cordis::detail::invoke_owned(
        [*this](std::string label,
                std::function<Async<void>(Context)> cleanup) -> Async<Effect> {
          // Publish before host registration: owner shutdown can start
          // immediately.
          auto callback = callbacks_->insert(std::move(cleanup));
          try {
            auto result = (co_await detail::receive(
                raw_.api, raw_.host, true,
                [&](uint64_t caller, CordisAbiCompletion completion) {
                  raw_.api->defer(raw_.host, raw_.grant, callback,
                                  detail::slice(label), caller, completion);
                }));
            co_return Effect(raw_.host, result.second, raw_.api);
          } catch (const Rejected &) {
            callbacks_->erase(callback);
            throw;
          }
          // Unknown outcomes retain the callback and prevent instance
          // destruction.
        },
        std::move(label), std::move(cleanup));
  }
};

inline uint64_t detail::Callbacks::insert_managed(Control call) {
  return insert_payload([call =
                             std::move(call)](uint32_t op, Context ctx,
                                              Payload args) -> Async<Payload> {
    co_return Payload(co_await call(op, std::move(ctx), args.control_only()));
  });
}

struct ContextResource {
  virtual ~ContextResource() = default;
  virtual Async<Message> invoke(Context ctx, std::string method,
                                Message args) = 0;
  virtual Async<void> release(Context ctx) = 0;
};
struct ContextService {
  virtual ~ContextService() = default;
  virtual Async<void> activate(Context) { co_return; }
  virtual Async<Message> invoke(Context ctx, std::string method,
                                Message args) = 0;
  virtual Async<void> deactivate(Context) { co_return; }
  virtual Async<std::shared_ptr<ContextResource>> acquire(Context, std::string,
                                                          Message) {
    co_return co_await ::cordis::detail::failed<std::shared_ptr<ContextResource>>(
        Refusal("unsupported", "resource kind unsupported"));
  }
};

// Model: contract_name(), contract_revision(), interface(), dependencies(),
// dependency_interfaces() -> object,
// create(Value) -> shared_ptr<ContextService>. Business methods must be thread
// safe, including reentry. Metadata locks never span user calls or host calls.
template <class Model> class ContextHost {
  struct Instance {
    native::Runtime runtime;
    std::mutex mutex;
    std::size_t ordinary = 0, cleanup = 0;
    std::shared_ptr<ContextService> service;
    std::shared_ptr<detail::Callbacks> callbacks =
        std::make_shared<detail::Callbacks>();
    std::unordered_map<uint64_t, std::shared_ptr<ContextResource>> resources;
    bool attempted = false, active = false, deactivated = false;
  };
  std::mutex mutex;
  uint64_t next = 1;
  std::unordered_map<uint64_t, std::shared_ptr<Instance>> instances;
  static ContextHost &host() {
    static ContextHost h;
    return h;
  }
  static uint64_t id() {
    auto &h = host();
    std::lock_guard<std::mutex> lock(h.mutex);
    if (h.next == std::numeric_limits<uint64_t>::max())
      throw Refusal("exhausted", "handle space exhausted");
    return h.next++;
  }
  static std::shared_ptr<Instance> get(uint64_t id) {
    auto &h = host();
    std::lock_guard<std::mutex> lock(h.mutex);
    auto found = h.instances.find(id);
    if (found == h.instances.end())
      throw Refusal("closed", "unknown instance");
    return found->second;
  }
  static CordisAbiResult create(const CordisAbiBuffer *config) noexcept {
    return detail::protect([&] {
      auto key = id();
      auto i = std::make_shared<Instance>();
      auto settings = detail::read(config, Validation::Audit).control_only();
      i->callbacks->validation = cordis::validation(settings.at("validation").get<std::string>());
      i->service = Model::create(settings.at("config"));
      if (!i->service)
        throw std::runtime_error("null service");
      auto &h = host();
      {
        std::lock_guard<std::mutex> lock(h.mutex);
        h.instances.emplace(key, i);
      }
      return detail::result(nullptr, key);
    });
  }
  static CordisAbiResult destroy(uint64_t key) noexcept {
    return detail::protect([&] {
      auto i = get(key);
      {
        std::lock_guard<std::mutex> lock(i->mutex);
        if (i->ordinary || i->cleanup || !i->resources.empty() ||
            !i->callbacks->empty() || (i->attempted && !i->deactivated))
          throw Refusal("owned", "instance not recovered");
      }
      i->runtime.close();
      auto &h = host();
      {
        std::lock_guard<std::mutex> lock(h.mutex);
        h.instances.erase(key);
      }
      return detail::result();
    });
  }
  static CordisAbiResult describe() noexcept {
    return detail::protect([] {
      auto deps = Model::dependencies(),
           interfaces = Model::dependency_interfaces();
      if (!deps.is_object() || !interfaces.is_object() ||
          deps.size() != interfaces.size())
        throw std::runtime_error(
            "dependency descriptor keys must match dependencies");
      for (auto it = deps.begin(); it != deps.end(); ++it)
        interfaces.at(it.key()) =
            ::cordis::contract::normalize(interfaces.at(it.key()));
      return detail::result(
          {{"dependencies", deps}, {"interfaces", interfaces}});
    });
  }
  static void submit(uint64_t key, uint32_t operation, uint64_t target,
                     CordisAbiContext raw, CordisAbiSlice method,
                     const CordisAbiBuffer *args,
                     const CordisAbiManagedHostApi *managed,
                     const CordisAbiReactiveHostApi *reactive,
                     const CordisAbiCompositionHostApi *composition,
                     const CordisAbiResourceHostApi *resources,
                     CordisAbiCompletion done) noexcept {
    std::shared_ptr<Instance> instance;
    const bool cleanup = operation == CORDIS_ABI_DEACTIVATE ||
                         operation == CORDIS_ABI_RESOURCE_RELEASE ||
                         operation == CORDIS_ABI_EFFECT_RELEASE ||
                         operation == CORDIS_ABI_SEQUENCE_UNDO ||
                         operation == CORDIS_ABI_SEQUENCE_RELEASE ||
                         operation == CORDIS_ABI_CALLBACK_RELEASE;
    bool admitted = false;
    try {
      instance = get(key);
      auto name = detail::text(method);
      auto value = detail::read(args, instance->callbacks->validation);
      {
        std::lock_guard lock(instance->mutex);
        auto &count = cleanup ? instance->cleanup : instance->ordinary;
        if (count >= 128)
          throw Refusal("overloaded", "callback capacity reached");
        ++count;
        admitted = true;
      }
      instance->runtime.submit(
          [instance, key, operation, target, raw, managed, reactive,
           composition, resources, cleanup, done, name = std::move(name),
           value = std::move(value)]() mutable -> Async<void> {
            CordisAbiResult result{};
            co_await detail::with_caller(
                {raw.host, raw.grant, instance->callbacks->validation}, [&]() -> Async<void> {
                  result = co_await detail::protect_async([&] {
                    return managed_dispatch_impl(
                        key, operation, target, {raw, managed}, std::move(name),
                        std::move(value), reactive, composition, resources);
                  });
                });
            {
              std::lock_guard lock(instance->mutex);
              --(cleanup ? instance->cleanup : instance->ordinary);
            }
            detail::complete(done, result);
          });
    } catch (...) {
      if (admitted) {
        std::lock_guard lock(instance->mutex);
        --(cleanup ? instance->cleanup : instance->ordinary);
      }
      auto failure = std::current_exception();
      detail::complete(done, detail::protect([failure]() -> CordisAbiResult {
                         std::rethrow_exception(failure);
                       }));
    }
  }
  static Async<CordisAbiResult>
  dispatch_impl(uint64_t key, uint32_t operation, uint64_t target,
                CordisAbiContext raw, std::string method, Payload args,
                const CordisAbiManagedHostApi *managed,
                const CordisAbiReactiveHostApi *reactive = nullptr,
                const CordisAbiCompositionHostApi *composition = nullptr,
                const CordisAbiResourceHostApi *resources = nullptr) {

    if (operation != CORDIS_ABI_INVOKE && operation != CORDIS_ABI_ACQUIRE &&
        operation != CORDIS_ABI_RESOURCE_CALL)
      args.control_only();
    auto i = get(key);
    Context ctx(raw, i->callbacks, managed, reactive, composition, resources);
    // Runtime leases serialize lifecycle transitions and drain calls before
    // release. Checks/lookup use short locks; shared_ptr pins callback
    // targets.
    if (operation == CORDIS_ABI_ACTIVATE) {
      {
        std::lock_guard<std::mutex> lock(i->mutex);
        if (i->attempted)
          throw Refusal("state", "activation already attempted");
        i->attempted = true;
      }
      (co_await i->service->activate(ctx));
      {
        std::lock_guard<std::mutex> lock(i->mutex);
        i->active = true;
      }
      co_return detail::result();
    }
    if (operation == CORDIS_ABI_EFFECT_RELEASE) {
      auto cleanup = i->callbacks->get(target);
      (co_await cleanup(ctx));
      i->callbacks->erase(target);
      co_return detail::result();
    }
    if (operation == CORDIS_ABI_DEACTIVATE) {
      {
        std::lock_guard<std::mutex> lock(i->mutex);
        if (!i->resources.empty() || !i->callbacks->empty())
          throw Refusal("owned", "effects remain");
        if (i->deactivated)
          co_return detail::result();
        i->active = false;
      }
      (co_await i->service->deactivate(ctx));
      {
        std::lock_guard<std::mutex> lock(i->mutex);
        i->deactivated = true;
      }
      co_return detail::result();
    }
    std::shared_ptr<ContextResource> resource;
    {
      std::lock_guard<std::mutex> lock(i->mutex);
      if (!i->active)
        throw Refusal("closed", "instance inactive");
      if (operation == CORDIS_ABI_RESOURCE_CALL ||
          operation == CORDIS_ABI_RESOURCE_RELEASE) {
        auto found = i->resources.find(target);
        if (found == i->resources.end())
          throw Refusal("closed", "unknown resource");
        resource = found->second;
      }
    }
    switch (operation) {
    case CORDIS_ABI_INVOKE:
      co_return detail::result(
          (co_await i->service->invoke(ctx, method, args.one())));
    case CORDIS_ABI_ACQUIRE: {
      auto resource_id = id();
      auto value = (co_await i->service->acquire(ctx, method, args.one()));
      if (!value)
        throw std::runtime_error("null resource");
      {
        std::lock_guard<std::mutex> lock(i->mutex);
        i->resources.emplace(resource_id, value);
      }
      co_return detail::result(nullptr, resource_id);
    }
    case CORDIS_ABI_RESOURCE_CALL:
      co_return detail::result(
          (co_await resource->invoke(ctx, method, args.one())));
    case CORDIS_ABI_RESOURCE_RELEASE:
      (co_await resource->release(ctx));
      {
        std::lock_guard<std::mutex> lock(i->mutex);
        i->resources.erase(target);
      }
      co_return detail::result();
    default:
      throw Refusal("operation", "unknown Context operation");
    }
  }
  static void dispatch(uint64_t key, uint32_t operation, uint64_t target,
                       CordisAbiContext raw, CordisAbiSlice method,
                       const CordisAbiBuffer *args,
                       CordisAbiCompletion done) noexcept {
    submit(key, operation, target, raw, method, args, nullptr, nullptr, nullptr,
           nullptr, done);
  }
  static Async<CordisAbiResult> managed_dispatch_impl(
      uint64_t key, uint32_t operation, uint64_t target,
      CordisAbiManagedContext raw, std::string method, Payload args,
      const CordisAbiReactiveHostApi *reactive,
      const CordisAbiCompositionHostApi *composition = nullptr,
      const CordisAbiResourceHostApi *resources = nullptr) {
    if (operation <= CORDIS_ABI_EFFECT_RELEASE)
      co_return (co_await dispatch_impl(key, operation, target, raw.base,
                                        method, args, raw.managed, reactive,
                                        composition, resources));

    if (!raw.managed || operation < CORDIS_ABI_SEQUENCE_NEXT ||
        operation >
            (reactive ? CORDIS_ABI_CALLBACK_RELEASE : CORDIS_ABI_TASK_CALL))
      throw Refusal("operation", "unknown managed callback");
    auto i = get(key);
    auto call = i->callbacks->get_managed(target);
    bool terminal = operation == CORDIS_ABI_SEQUENCE_RELEASE ||
                    operation == CORDIS_ABI_SCOPE_CALL ||
                    operation == CORDIS_ABI_TASK_CALL ||
                    operation == CORDIS_ABI_CALLBACK_RELEASE;
    try {
      auto result =
          (co_await (*call)(operation,
                            Context(raw.base, i->callbacks, raw.managed,
                                    reactive, composition, resources),
                            args));
      if (terminal)
        i->callbacks->erase_managed(target);
      co_return detail::result(std::move(result));
    } catch (const Rejected &) {
      // A failed producer release must retain its closure. Ordinary known
      // task/run errors finish execution; native effects still own recovery.
      if (operation != CORDIS_ABI_SEQUENCE_RELEASE &&
          operation != CORDIS_ABI_CALLBACK_RELEASE && terminal)
        i->callbacks->erase_managed(target);
      throw;
    }
  }
  static void managed_dispatch(uint64_t key, uint32_t operation,
                               uint64_t target, CordisAbiManagedContext raw,
                               CordisAbiSlice method,
                               const CordisAbiBuffer *args,
                               CordisAbiCompletion done) noexcept {
    submit(key, operation, target, raw.base, method, args, raw.managed, nullptr,
           nullptr, nullptr, done);
  }
  static void reactive_dispatch(uint64_t key, uint32_t operation,
                                uint64_t target, CordisAbiReactiveContext raw,
                                CordisAbiSlice method,
                                const CordisAbiBuffer *args,
                                CordisAbiCompletion done) noexcept {
    submit(key, operation, target, raw.base.base, method, args,
           raw.base.managed, raw.reactive, nullptr, nullptr, done);
  }
  static CordisAbiResult describe_reactive() noexcept {
    return detail::protect([] { return detail::result(Model::bindings()); });
  }
  static void composition_dispatch(uint64_t key, uint32_t operation,
                                   uint64_t target,
                                   CordisAbiCompositionContext raw,
                                   CordisAbiSlice method,
                                   const CordisAbiBuffer *args,
                                   CordisAbiCompletion done) noexcept {
    submit(key, operation, target, raw.base, method, args, raw.managed,
           raw.reactive, raw.composition, nullptr, done);
  }
  static CordisAbiResult describe_composition() noexcept {
    return detail::protect([] { return detail::result(Model::composition()); });
  }
  static void resource_dispatch(uint64_t key, uint32_t operation,
                                uint64_t target, CordisAbiResourceContext raw,
                                CordisAbiSlice method,
                                const CordisAbiBuffer *args,
                                CordisAbiCompletion done) noexcept {
    submit(key, operation, target, raw.base.base, method, args,
           raw.base.managed, raw.base.reactive, raw.base.composition,
           raw.resources, done);
  }
  static void required(uint64_t, CordisAbiCompletion done) noexcept {
    detail::complete(done, detail::protect([]() -> CordisAbiResult {
                       throw Refusal("context_required",
                                     "service requires an invocation Context");
                     }));
  }
  static void required_call(uint64_t key, CordisAbiSlice,
                            const CordisAbiBuffer *,
                            CordisAbiCompletion done) noexcept {
    required(key, done);
  }
  static void required_resource(uint64_t key, uint64_t, CordisAbiSlice,
                                const CordisAbiBuffer *,
                                CordisAbiCompletion done) noexcept {
    required(key, done);
  }
  static void required_release(uint64_t key, uint64_t,
                               CordisAbiCompletion done) noexcept {
    required(key, done);
  }
  static void unscoped_deactivate(uint64_t key,
                                  CordisAbiCompletion done) noexcept {
    detail::complete(done, detail::protect([&] {
                       auto instance = get(key);
                       std::lock_guard lock(instance->mutex);
                       if (instance->attempted)
                         throw Refusal(
                             "context_required",
                             "service requires an invocation Context");
                       return detail::result();
                     }));
  }

public:
  static uint32_t resource_entry(uint32_t version, CordisAbiResourceApi *out,
                                 size_t size) noexcept {
    if (version != CORDIS_ABI_RESOURCES_VERSION ||
        size != sizeof(CordisAbiResourceApi) || !out)
      return 1;
    *out = {CORDIS_ABI_RESOURCES_VERSION, sizeof(CordisAbiResourceApi),
            resource_dispatch};
    return 0;
  }
  static CordisAbiResult describe_interface() noexcept {
    return detail::protect([] {
      return detail::result(::cordis::contract::normalize(Model::interface()));
    });
  }
  static uint32_t entry(uint32_t version, CordisAbiApi *out,
                        size_t size) noexcept {
    if (version != CORDIS_ABI_VERSION || size != sizeof(CordisAbiApi) || !out)
      return 1;
    try {
      static const std::string name = Model::contract_name(),
                               revision = Model::contract_revision();
      *out = {CORDIS_ABI_VERSION,
              sizeof(CordisAbiApi),
              CORDIS_ABI_RESOURCES,
              detail::slice(name),
              detail::slice(revision),
              create,
              required,
              required_call,
              unscoped_deactivate,
              destroy,
              detail::release,
              required_call,
              required_resource,
              required_release,
              describe_interface};
      return 0;
    } catch (...) {
      return 1;
    }
  }
  static uint32_t composition_entry(uint32_t version,
                                    CordisAbiCompositionApi *out,
                                    size_t size) noexcept {
    if (version != CORDIS_ABI_COMPOSITION_VERSION ||
        size != sizeof(CordisAbiCompositionApi) || !out)
      return 1;
    *out = {CORDIS_ABI_COMPOSITION_VERSION, sizeof(CordisAbiCompositionApi),
            describe_composition, composition_dispatch};
    return 0;
  }
  static uint32_t managed_entry(uint32_t version, CordisAbiManagedApi *out,
                                size_t size) noexcept {
    if (version != CORDIS_ABI_MANAGED_VERSION ||
        size != sizeof(CordisAbiManagedApi) || !out)
      return 1;
    *out = {CORDIS_ABI_MANAGED_VERSION, sizeof(CordisAbiManagedApi),
            managed_dispatch};
    return 0;
  }
  static uint32_t reactive_entry(uint32_t version, CordisAbiReactiveApi *out,
                                 size_t size) noexcept {
    if (version != CORDIS_ABI_REACTIVE_VERSION ||
        size != sizeof(CordisAbiReactiveApi) || !out)
      return 1;
    *out = {CORDIS_ABI_REACTIVE_VERSION, sizeof(CordisAbiReactiveApi),
            describe_reactive, reactive_dispatch};
    return 0;
  }
  static uint32_t context_entry(uint32_t version, CordisAbiContextApi *out,
                                size_t size) noexcept {
    if (version != CORDIS_ABI_CONTEXT_VERSION ||
        size != sizeof(CordisAbiContextApi) || !out)
      return 1;
    *out = {CORDIS_ABI_CONTEXT_VERSION, sizeof(CordisAbiContextApi), describe,
            dispatch};
    return 0;
  }
};
} // namespace cordis::abi
