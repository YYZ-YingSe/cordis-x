#pragma once
#include <bit>
#include <cmath>
#include <cordis/abi.h>
#include <cordis/core/contract.hpp>
#include <cordis/core/error.hpp>
#include <cordis/core/value.hpp>
#include <cordis/protocols/session/binary.hpp>
#include <cordis/runtime/native.hpp>
#include <limits>
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include <stdexcept>
#include <string>
#include <string_view>
#include <unordered_map>
#include <vector>

namespace cordis::abi {
using ::cordis::Flow;
using ::cordis::max_integer;
using ::cordis::Value;
using ::cordis::session::Payload;
using Rejected = ::cordis::RpcError;
struct Resource {
  virtual ~Resource() = default;
  virtual Async<Message> invoke(std::string method, Message args) = 0;
  virtual Async<void> release() = 0;
};
struct Service {
  virtual ~Service() = default;
  virtual Async<void> activate() { co_return; }
  virtual Async<Message> invoke(std::string method, Message args) = 0;
  virtual Async<void> deactivate() { co_return; }
  virtual Async<std::unique_ptr<Resource>> acquire(std::string, Message) {
    co_return co_await ::cordis::detail::failed<std::unique_ptr<Resource>>(
        Refusal("unsupported", "resource kind unsupported"));
  }
};

namespace detail {
inline CordisAbiSlice slice(const std::string &value) {
  return {reinterpret_cast<const uint8_t *>(value.data()), value.size()};
}
inline std::string text(CordisAbiSlice value) {
  if (value.len == 0)
    return {};
  if (!value.data)
    throw std::runtime_error("null ABI string");
  return {reinterpret_cast<const char *>(value.data), value.len};
}
struct Budget {
  std::size_t left = session::max_message;
  void take(std::size_t count) {
    if (count > left)
      throw std::runtime_error("ABI buffer exceeds budget");
    left -= count;
  }
};
inline Payload read(const CordisAbiBuffer &raw, Budget &budget,
                    Validation validation = Validation::Checked) {
  budget.take(raw.len);
  if (raw.len && !raw.data)
    throw std::runtime_error("null ABI buffer");
  // Standard protobuf decoding copies the borrowed buffer into owned messages.
  return session::decode_payload(raw.data, raw.len, validation);
}
inline Payload read(const CordisAbiBuffer *raw,
                    Validation validation = Validation::Checked) {
  if (!raw)
    throw std::runtime_error("missing ABI buffer");
  Budget budget;
  return read(*raw, budget, validation);
}
struct Output {
  std::vector<uint8_t> bytes;
  std::string code, message;
  CordisAbiBuffer build(const Payload &value, Budget &budget) {
    bytes = session::encode(value);
    budget.take(bytes.size());
    return {bytes.data(), bytes.size()};
  }
  CordisAbiBuffer build(const Value &value, Budget &budget) {
    return build(Payload(value), budget);
  }
  CordisAbiBuffer build(const Message &value, Budget &budget) {
    return build(Payload(value), budget);
  }
};
inline CordisAbiResult result(Payload value, uint64_t handle = 0,
                              uint32_t status = CORDIS_ABI_OK,
                              std::string code = {}, std::string message = {}) {
  auto storage = std::make_unique<Output>();
  storage->code = std::move(code);
  storage->message = std::move(message);
  Budget budget;
  CordisAbiBuffer raw{};
  if (status == CORDIS_ABI_OK)
    raw = storage->build(value, budget);
  budget.take(storage->code.size());
  budget.take(storage->message.size());
  CordisAbiResult output{status,
                         0,
                         raw,
                         slice(storage->code),
                         slice(storage->message),
                         handle,
                         storage.get()};
  storage.release();
  return output;
}
inline CordisAbiResult result(Value value = nullptr, uint64_t handle = 0,
                              uint32_t status = CORDIS_ABI_OK,
                              std::string code = {}, std::string message = {}) {
  return result(Payload(std::move(value)), handle, status, std::move(code),
                std::move(message));
}
inline CordisAbiResult result(Message value) {
  return result(Payload(std::move(value)));
}
// The fallback itself cannot allocate or throw, including on std::bad_alloc.
inline CordisAbiResult fallback() noexcept {
  static const std::string_view code = "exception",
                                message = "native operation outcome unknown";
  CordisAbiResult output{};
  output.status = CORDIS_ABI_UNKNOWN;
  output.code = {reinterpret_cast<const uint8_t *>(code.data()), code.size()};
  output.message = {reinterpret_cast<const uint8_t *>(message.data()),
                    message.size()};
  return output;
}
template <class F> CordisAbiResult protect(F &&call) noexcept {
  try {
    return call();
  } catch (const Refusal &error) {
    try {
      return result(nullptr, 0, CORDIS_ABI_REFUSED, error.code, error.what());
    } catch (...) {
      return fallback();
    }
  } catch (const Rejected &error) {
    try {
      return result(nullptr, 0, CORDIS_ABI_REJECTED, error.code, error.what());
    } catch (...) {
      return fallback();
    }
  } catch (...) {
    return fallback();
  }
}
inline void release(CordisAbiResult *result) noexcept {
  delete static_cast<Output *>(result->owner);
  *result = {};
}
template <class F> Async<CordisAbiResult> protect_async(F call) {
  try {
    co_return co_await call();
  } catch (...) {
    auto failure = std::current_exception();
    co_return protect(
        [failure]() -> CordisAbiResult { std::rethrow_exception(failure); });
  }
}
struct Caller {
  uint64_t host, grant;
  Validation validation = Validation::Checked;
};
inline std::mutex callers_mutex;
inline std::unordered_map<corral::Executor *, Caller> callers;
// Each exported callback uses an upstream nursery/executor as its context key.
// Nested coroutines inherit it; unrelated calls on the same thread never share
// caller identity. No custom coroutine scheduler or thread-local active grant.
inline Async<void> with_caller(Caller caller,
                               std::function<Async<void>()> body) {
  corral::UnsafeNursery root(native::Runtime::current().io());
  std::exception_ptr failure;
  root.start([&]() -> Async<void> {
    try {
      auto *executor = co_await corral::getExecutor();
      {
        std::lock_guard lock(callers_mutex);
        callers.emplace(executor, caller);
      }
      struct Guard {
        corral::Executor *executor;
        ~Guard() {
          std::lock_guard lock(callers_mutex);
          callers.erase(executor);
        }
      } guard{executor};
      co_await body();
    } catch (...) {
      // start() can run the body before join() adopts this nursery. Hold an
      // immediate exception until adoption and callback-tail retirement finish.
      failure = std::current_exception();
    }
  });
  co_await corral::try_([&]() -> Async<void> {
    co_await root.join();
  }).finally([]() -> Async<void> {
    // A callback portal may still be unwinding Executor::capture after
    // join wakes its parent. Retire this root on the next Asio turn so
    // its executor outlives that callback tail, including cancellation.
    co_await native::completed<bool>([](auto done) { done(true); });
  });
  if (failure)
    std::rethrow_exception(failure);
}
inline Async<Caller> active_caller() {
  auto *executor = co_await corral::getExecutor();
  std::lock_guard lock(callers_mutex);
  auto found = callers.find(executor);
  if (found == callers.end())
    throw Refusal("closed", "ABI handle requires an executing callback");
  co_return found->second;
}
inline Async<Caller> current_caller(uint64_t host) {
  auto caller = co_await active_caller();
  if (caller.host != host)
    throw Refusal("closed",
                  "ABI handle requires a callback of its owning instance");
  co_return caller;
}
inline void complete(CordisAbiCompletion completion,
                     CordisAbiResult result) noexcept {
  completion.complete(completion.user_data, result);
}
} // namespace detail

// Model supplies local metadata/factory; every business operation is
// asynchronous. destroy is the join boundary that makes it safe to unload this
// module.
template <class Model> class Host {
  struct Instance {
    Validation validation = Validation::Checked;
    native::Runtime runtime;
    std::unique_ptr<Service> service;
    std::unordered_map<uint64_t, std::shared_ptr<Resource>> resources;
    bool attempted = false, active = false, deactivated = false;
  };
  std::mutex mutex;
  uint64_t next = 1;
  std::unordered_map<uint64_t, std::shared_ptr<Instance>> instances;
  static Host &host() {
    static Host h;
    return h;
  }
  static uint64_t id() {
    auto &h = host();
    std::lock_guard lock(h.mutex);
    if (h.next == std::numeric_limits<uint64_t>::max())
      throw Refusal("exhausted", "handle space exhausted");
    return h.next++;
  }
  static std::shared_ptr<Instance> get(uint64_t id) {
    auto &h = host();
    std::lock_guard lock(h.mutex);
    auto it = h.instances.find(id);
    if (it == h.instances.end())
      throw Refusal("closed", "unknown instance");
    return it->second;
  }
  static CordisAbiResult create(const CordisAbiBuffer *config) noexcept {
    return detail::protect([&] {
      auto key = id();
      auto instance = std::make_shared<Instance>();
      auto settings = detail::read(config, Validation::Audit).control_only();
      instance->validation = cordis::validation(settings.at("validation").get<std::string>());
      instance->service = Model::create(settings.at("config"));
      if (!instance->service)
        throw std::runtime_error("null factory result");
      auto &h = host();
      std::lock_guard lock(h.mutex);
      h.instances.emplace(key, std::move(instance));
      return detail::result(nullptr, key);
    });
  }
  template <class F>
  static void submit(uint64_t key, CordisAbiCompletion completion,
                     F run) noexcept {
    try {
      auto instance = get(key);
      instance->runtime.submit([instance, run = std::move(run),
                                completion]() mutable -> Async<void> {
        auto result =
            co_await detail::protect_async([&] { return run(*instance); });
        detail::complete(completion, result);
      });
    } catch (...) {
      auto failure = std::current_exception();
      detail::complete(completion,
                       detail::protect([failure]() -> CordisAbiResult {
                         std::rethrow_exception(failure);
                       }));
    }
  }
  static void activate(uint64_t key, CordisAbiCompletion done) noexcept {
    submit(key, done, [](Instance &i) -> Async<CordisAbiResult> {
      if (i.attempted)
        throw Refusal("state", "activation already attempted");
      i.attempted = true;
      co_await i.service->activate();
      i.active = true;
      co_return detail::result();
    });
  }
  static void active(const Instance &i) {
    if (!i.active)
      throw Refusal("closed", "instance inactive");
  }
  template <class F>
  static void input(uint64_t key, CordisAbiSlice method,
                    const CordisAbiBuffer *args, CordisAbiCompletion done,
                    F run) noexcept {
    try {
      auto name = detail::text(method);
      auto instance = get(key);
      auto value = detail::read(args, instance->validation).one();
      submit(key, done,
             [name = std::move(name), value = std::move(value),
              run](Instance &i) mutable {
               return run(i, std::move(name), std::move(value));
             });
    } catch (...) {
      auto failure = std::current_exception();
      detail::complete(done, detail::protect([failure]() -> CordisAbiResult {
                         std::rethrow_exception(failure);
                       }));
    }
  }
  static void invoke(uint64_t key, CordisAbiSlice method,
                     const CordisAbiBuffer *args,
                     CordisAbiCompletion done) noexcept {
    input(key, method, args, done,
          [](Instance &i, std::string name,
             Message value) -> Async<CordisAbiResult> {
            active(i);
            co_return detail::result(
                co_await i.service->invoke(std::move(name), std::move(value)));
          });
  }
  static void acquire(uint64_t key, CordisAbiSlice method,
                      const CordisAbiBuffer *args,
                      CordisAbiCompletion done) noexcept {
    input(key, method, args, done,
          [](Instance &i, std::string name,
             Message value) -> Async<CordisAbiResult> {
            active(i);
            auto resource_id = id();
            auto resource =
                co_await i.service->acquire(std::move(name), std::move(value));
            if (!resource)
              throw std::runtime_error("null resource");
            i.resources.emplace(resource_id, std::move(resource));
            co_return detail::result(nullptr, resource_id);
          });
  }
  static void resource_call(uint64_t key, uint64_t resource,
                            CordisAbiSlice method, const CordisAbiBuffer *args,
                            CordisAbiCompletion done) noexcept {
    input(key, method, args, done,
          [resource](Instance &i, std::string name,
                     Message value) -> Async<CordisAbiResult> {
            active(i);
            auto found = i.resources.find(resource);
            if (found == i.resources.end())
              throw Refusal("closed", "unknown resource");
            auto object = found->second;
            co_return detail::result(
                co_await object->invoke(std::move(name), std::move(value)));
          });
  }
  static void resource_release(uint64_t key, uint64_t resource,
                               CordisAbiCompletion done) noexcept {
    submit(key, done, [resource](Instance &i) -> Async<CordisAbiResult> {
      auto found = i.resources.find(resource);
      if (found == i.resources.end())
        throw Refusal("closed", "unknown resource");
      auto object = found->second;
      co_await object->release();
      i.resources.erase(resource);
      co_return detail::result();
    });
  }
  static void deactivate(uint64_t key, CordisAbiCompletion done) noexcept {
    submit(key, done, [](Instance &i) -> Async<CordisAbiResult> {
      if (!i.resources.empty())
        throw Refusal("owned", "resources remain");
      i.active = false;
      if (!i.deactivated) {
        co_await i.service->deactivate();
        i.deactivated = true;
      }
      co_return detail::result();
    });
  }
  static CordisAbiResult destroy(uint64_t key) noexcept {
    return detail::protect([&] {
      auto instance = get(key);
      // Host lifecycle authority has drained all operations before destroy.
      // The join also covers code executing after the last completion signal.
      if (!instance->resources.empty() ||
          (instance->attempted && !instance->deactivated))
        throw Refusal("owned", "instance not recovered");
      instance->runtime.close();
      instance->service.reset();
      auto &h = host();
      std::lock_guard lock(h.mutex);
      h.instances.erase(key);
      return detail::result();
    });
  }
  static CordisAbiResult describe_interface() noexcept {
    return detail::protect([] {
      return detail::result(::cordis::contract::normalize(Model::interface()));
    });
  }

public:
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
              activate,
              invoke,
              deactivate,
              destroy,
              detail::release,
              acquire,
              resource_call,
              resource_release,
              describe_interface};
      return 0;
    } catch (...) {
      return 1;
    }
  }
};
} // namespace cordis::abi
