#pragma once
#include "context.hpp"
#include <algorithm>
#include <atomic>
#include <cordis/core/contract.hpp>
#include <cordis/protocols/http/server.hpp>
#include <cordis/transports/stdio.hpp>
#include <csignal>
#include <deque>
#include <functional>
#include <latch>
#include <map>
#include <mutex>
#include <set>
#include <thread>
#include <unistd.h>
#include <utility>

namespace cordis::process {
struct Resource {
  virtual ~Resource() = default;
  virtual Async<Message> invoke(std::string method, Message args) = 0;
  virtual Async<void> close() = 0;
  virtual Async<Message> invoke(std::string method, Message args, Context) {
    return invoke(std::move(method), std::move(args));
  }
  virtual Async<void> close(Context) { return close(); }
};
struct Service {
  virtual bool typed_dispatch() const { return false; }
  virtual ~Service() = default;
  virtual Json contract() const = 0;
  virtual Json interface() const = 0;
  virtual Json dependency_interfaces() const { return Json::object(); }
  virtual Json events() const { return Json::object(); }
  virtual Json middleware() const { return Json::object(); }
  virtual Json composition() const {
    return {{"factories", Json::array()}, {"provisions", Json::object()}};
  }
  virtual Json dependencies() const { return Json::object(); }
  virtual Async<void> activate() { co_return; }
  virtual Async<void> activate(Context) { return activate(); }
  virtual Async<Message> invoke(std::string method, Message) {
    throw Refusal("unknown_method", method);
    co_return Message("");
  }
  virtual Async<Message> invoke(std::string method, Message args, Context) {
    return invoke(std::move(method), std::move(args));
  }
  virtual Async<std::unique_ptr<Resource>> acquire(std::string, Message) {
    throw Refusal("unknown_resource_kind",
                  "service does not acquire resources");
    co_return nullptr;
  }
  virtual Async<std::unique_ptr<Resource>> acquire(std::string kind,
                                                   Message args, Context) {
    return acquire(std::move(kind), std::move(args));
  }
  virtual Async<void> deactivate() { co_return; }
  virtual Async<void> deactivate(Context) { return deactivate(); }
};

// Separate admission lanes share one Asio/Corral execution environment. A
// suspended operation retains its lane permit without occupying a worker.
class Lane {
  native::Runtime &runtime;
  const std::size_t limit;
  std::atomic<std::size_t> pending{0};

public:
  explicit Lane(native::Runtime &runtime, std::size_t limit = 16)
      : runtime(runtime), limit(limit) {}
  bool post(std::function<Async<void>()> task) {
    if (pending.fetch_add(1) >= limit) {
      --pending;
      return false;
    }
    try {
      runtime.submit([this, task = std::move(task)]() -> Async<void> {
        struct Done {
          Lane &lane;
          ~Done() { --lane.pending; }
        } done{*this};
        co_await task();
      });
      return true;
    } catch (...) {
      --pending;
      return false;
    }
  }
};

class Host {
  struct ResourceRecord {
    std::string kind;
    std::string owner;
    std::unique_ptr<Resource> resource;
    std::mutex mutex;
    std::size_t calls = 0;
    bool closing = false, finished = false;
    std::exception_ptr failure;
    corral::Event drained_calls, completed;
    Async<Message> invoke(std::string method, Message args, Context ctx) {
      {
        std::lock_guard lock(mutex);
        if (closing)
          throw Refusal("closed", "resource is closing");
        ++calls;
      }
      struct Guard {
        ResourceRecord &entry;
        ~Guard() {
          bool drained;
          {
            std::lock_guard lock(entry.mutex);
            drained = --entry.calls == 0 && entry.closing;
          }
          if (drained)
            entry.drained_calls.trigger();
        }
      } guard{*this};
      co_return co_await native::with_cleanup(
          [&] {
            return resource->invoke(std::move(method), std::move(args), ctx);
          },
          [ctx] { return ctx.finish(); });
    }
    Async<void> release(Context ctx) {
      std::unique_lock lock(mutex);
      if (closing) {
        lock.unlock();
        co_await completed;
        if (failure)
          std::rethrow_exception(failure);
        co_return;
      }
      closing = true;
      const bool wait = calls != 0;
      lock.unlock();
      if (wait)
        co_await drained_calls;
      std::exception_ptr error;
      try {
        co_await native::with_cleanup([&] { return resource->close(ctx); },
                                      [ctx] { return ctx.finish(); });
        resource.reset();
      } catch (...) {
        error = std::current_exception();
      }
      lock.lock();
      failure = error;
      finished = true;
      lock.unlock();
      completed.trigger();
      if (failure)
        std::rethrow_exception(failure);
    }
    bool released() {
      std::lock_guard lock(mutex);
      return finished && !failure;
    }
  };
  Service &service;
  const Json descriptor;
  std::mutex state_mutex;
  corral::Event drained;
  std::string session;
  Identity activation;
  bool greeted = false, active = false, activation_attempted = false;
  std::size_t inflight = 0;
  std::set<std::string> seen;
  std::map<std::string, std::shared_ptr<ResourceRecord>> resources;
  std::uint64_t next_resource = 0;
  native::Runtime runtime;
  http::Server http_server{runtime, [this](Request request) {
                             return dispatch(std::move(request));
                           }, [this] { return greeted ? dependencies->validation : Validation::Audit; }};
  transports::Stdio stdio{runtime, [this] { stop(1, true); }};
  bool closed = false;
  std::string url;
  bool enable_http;
  std::shared_ptr<DependencyChannel> dependencies =
      std::make_shared<DependencyChannel>();
  std::atomic<int> exit_status{-1};
  std::shared_ptr<EffectRegistry> effects = std::make_shared<EffectRegistry>();
  bool effects_enabled = false, sequences_enabled = false,
       scopes_enabled = false, tasks_enabled = false, events_enabled = false,
       middleware_enabled = false, composition_enabled = false;
  Json composition;
  std::shared_ptr<SequenceRegistry> sequences =
      std::make_shared<SequenceRegistry>();
  std::shared_ptr<ScopeRegistry> scopes = std::make_shared<ScopeRegistry>();
  std::shared_ptr<TaskRegistry> tasks = std::make_shared<TaskRegistry>();
  std::shared_ptr<EventRegistry> events;
  std::shared_ptr<MiddlewareRegistry> middlewares;
  Lane middleware_lane{runtime};
  Lane terminal_lane{runtime};
  Lane event_lane{runtime};
  Lane task_lane{runtime};
  Lane ordinary_lane{runtime}; // shares the executor with callback lanes
  Lane step_lane{runtime};
  // Match scope_run's admission bound so a nested run cannot queue behind
  // sixteen waiting callers; deeper excess is rejected by the channel.
  Lane scope_lane{runtime};
  Lane cleanup_lane{
      runtime}; // cleanup remains admissible during ordinary waits

  template <class F>
  static auto checked_business(const Json &errors, F call) -> decltype(call()) {
    try {
      co_return co_await call();
    } catch (const Refusal &) {
      throw;
    } catch (const RpcError &e) {
      if (e.code.empty() ||
          (std::find(errors.begin(), errors.end(), e.code) == errors.end() &&
           std::find(errors.begin(), errors.end(), "*") == errors.end()))
        throw std::runtime_error("undeclared business error: " + e.code);
      throw;
    }
  }
  static bool work(const std::string &command) {
    return command == "call" || command == "resource_acquire" ||
           command == "resource_call" || command == "resource_release";
  }
  static bool resource_id(const std::string &id) {
    return id.size() >= 2 && id.size() <= 21 && id[0] == 'r' &&
           std::all_of(id.begin() + 1, id.end(),
                       [](char c) { return c >= '0' && c <= '9'; });
  }
  Async<Payload> resource_operation(std::string command, Payload input,
                                    Context ctx) {
    const auto &payload = input.control;
    const auto owner = payload.at("owner").get<std::string>();
    if (owner.size() != 32 ||
        !std::all_of(owner.begin(), owner.end(), [](char c) {
          return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
        }))
      throw Refusal("invalid_argument", "invalid resource ownership grant");
    if (command == "resource_acquire") {
      const auto kind = payload.at("kind").get<std::string>();
      try {
        if (!service.typed_dispatch()) cordis::contract::check_value(
            cordis::contract::operation(descriptor.at("resources"), kind)
                .at("args"),
            input.one(), "resources." + kind + ".args", ctx.validation());
      } catch (const std::exception &e) {
        throw Refusal("invalid_argument", e.what());
      }
      std::string id;
      {
        std::lock_guard lock(state_mutex);
        if (next_resource >= 4096)
          throw Refusal("resources_exhausted",
                        "resource history full; restart session");
        id = "r" + std::to_string(++next_resource);
      }
      auto resource = (co_await checked_business(
          cordis::contract::operation(descriptor.at("resources"), kind)
              .at("errors"),
          [&] { return service.acquire(kind, input.one(), ctx); }));
      if (!resource)
        throw std::runtime_error("acquire returned no resource");
      auto entry = std::make_shared<ResourceRecord>();
      entry->owner = owner;
      entry->kind = kind;
      entry->resource = std::move(resource);
      {
        std::lock_guard lock(state_mutex);
        resources.emplace(id, std::move(entry));
      }
      co_return Json{{"resource", id}, {"owner", owner}};
    }
    const auto id = payload.at("resource").get<std::string>();
    std::shared_ptr<ResourceRecord> entry;
    {
      std::lock_guard lock(state_mutex);
      const auto found = resources.find(id);
      if (found == resources.end())
        throw Refusal("unknown_resource", "resource not in this activation");
      entry = found->second;
    }
    if (entry->owner != owner)
      throw Refusal("resource_owner_mismatch",
                    "resource belongs to a different grant");
    if (command == "resource_release") {
      (co_await entry->release(ctx));
      co_return Json::object();
    }
    const auto method = payload.at("method").get<std::string>();
    Json signature;
    try {
      signature = cordis::contract::operation(
          cordis::contract::operation(descriptor.at("resources"), entry->kind)
              .at("methods"),
          method);
      if (!service.typed_dispatch()) cordis::contract::check_value(signature.at("args"), input.one(),
                                    "resources." + entry->kind + "." + method +
                                        ".args", ctx.validation());
    } catch (const std::exception &e) {
      throw Refusal("invalid_argument", e.what());
    }
    auto result = (co_await checked_business(signature.at("errors"), [&] {
      return entry->invoke(method, input.one(), ctx);
    }));
    if (!service.typed_dispatch()) cordis::contract::check_value(signature.at("result"), result,
                                  "resource result", ctx.validation());
    co_return Payload(result);
  }

  static bool decimal(const Json &value) {
    if (!value.is_string())
      return false;
    const auto text = value.get<std::string>();
    if (text.empty() || text.size() > 20 || (text.size() > 1 && text[0] == '0'))
      return false;
    if (!std::all_of(text.begin(), text.end(),
                     [](char c) { return c >= '0' && c <= '9'; }))
      return false;
    return text.size() < 20 || text <= "18446744073709551615";
  }
  static void shape(const Request &request) {
    if (!decimal(request.operation) ||
        !decimal(request.activation.incarnation) ||
        !decimal(request.activation.fiber) ||
        !decimal(request.activation.epoch))
      throw Refusal("protocol", "invalid identity");
    if (request.session.size() != 32 ||
        !std::all_of(request.session.begin(), request.session.end(),
                     [](char c) {
                       return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
                     }))
      throw Refusal("protocol", "invalid session token");
  }
  Response failure(const Request &request, const std::string &code,
                   const std::string &message,
                   const char *kind = "failure") const {
    return response(request, Outcome{std::string(kind) == "refused"
                                         ? Outcome::Kind::refused
                                         : Outcome::Kind::failure,
                                     {},
                                     code,
                                     message});
  }
  void output(const Response &response) {
    stdio.write(encode(Frame(response)));
  }
  void remember(const Request &request) {
    if (seen.size() >= 65536)
      throw Refusal("session_exhausted", "operation history full");
    if (!seen.insert(request.operation).second)
      throw Refusal("duplicate_operation", "operation already admitted");
  }
  // dispatch owns the request until execution and its Context drain finish.
  Async<Payload> execute(const Request &request) {
    shape(request);
    const auto &command = request.command;
    const std::size_t bodies =
        command == "call" || command == "resource_acquire" ||
                command == "resource_call" || command == "event_call" ||
                command == "middleware_call" || command == "middleware_terminal"
            ? 1
            : 0;
    if (request.payload.messages.size() != bodies)
      throw Refusal("invalid_argument", "unexpected business message count");
    const auto &payload = request.payload.control;
    if (command == "hello") {
      try {
        cordis::contract::compare(descriptor, payload.at("interface"));
        const auto declared = service.dependency_interfaces();
        if (!declared.is_object() ||
            declared.size() != service.dependencies().size() ||
            !payload.at("dependency_interfaces").is_object() ||
            declared.size() != payload.at("dependency_interfaces").size())
          throw std::runtime_error("dependency interface identities differ");
        for (auto &item : declared.items()) {
          if (!service.dependencies().contains(item.key()))
            throw std::runtime_error("undeclared dependency interface");
          try {
            cordis::contract::compare(
                item.value(),
                payload.at("dependency_interfaces").at(item.key()));
          } catch (const std::exception &e) {
            throw std::runtime_error("dependencies." + item.key() + ": " +
                                     e.what());
          }
        }
      } catch (const std::exception &e) {
        throw Refusal("contract_mismatch", e.what());
      }
      std::lock_guard lock(state_mutex);
      if (greeted)
        throw Refusal("protocol", "hello must be first encoded");
      dependencies->validation = cordis::validation(payload.at("validation").get<std::string>());
      if (payload.at("contract") != service.contract())
        throw Refusal("contract_mismatch", "exact contract revision required");
      if (payload.value("dependencies", Json::object()) !=
          service.dependencies())
        throw Refusal("dependency_mismatch",
                      "declared dependencies must match installation");
      composition = check_composition(service.composition());
      if (check_composition(payload.value(
              "composition", Json{{"factories", Json::array()},
                                  {"provisions", Json::object()}})) !=
          composition)
        throw Refusal("composition_mismatch",
                      "composition bindings must match installation");
      composition_enabled = payload.value("capabilities", Json::object())
                                .value("composition", false);
      session = request.session;
      activation = request.activation;
      remember(request);
      greeted = true;
      effects_enabled =
          payload.value("capabilities", Json::object()).value("effects", false);
      dependencies->configure(session, activation,
                              [this](std::vector<uint8_t> bytes) {
                                stdio.write(std::move(bytes));
                              });
      sequences_enabled = payload.value("capabilities", Json::object())
                              .value("effect_sequences", false);
      tasks_enabled =
          payload.value("capabilities", Json::object()).value("tasks", false);
      events_enabled =
          payload.value("capabilities", Json::object()).value("events", false);
      middleware_enabled = payload.value("capabilities", Json::object())
                               .value("middleware", false);
      dependencies->enable_metadata(
          payload.value("capabilities", Json::object())
              .value("metadata", false));
      dependencies->reserve_cleanup(tasks_enabled || events_enabled ||
                                    middleware_enabled);
      scopes_enabled =
          payload.value("capabilities", Json::object()).value("scopes", false);
      const auto bound_events = payload.value("events", Json::object());
      const auto declared_events = service.events();
      if (!bound_events.is_object() ||
          bound_events.size() != declared_events.size())
        throw Refusal("event_mismatch",
                      "event bindings must match installation");
      for (const auto &item : declared_events.items()) {
        if (!bound_events.contains(item.key()))
          throw Refusal("event_mismatch", "missing event binding");
        const auto &bound = bound_events.at(item.key());
        if (bound.at("name") != item.value().at("name") ||
            bound.at("revision") != item.value().at("revision") ||
            !decimal(bound.at("realm")))
          throw Refusal("event_mismatch",
                        "event bindings must match installation");
      }
      const auto bound_middleware = payload.value("middleware", Json::object());
      const auto declared_middleware = service.middleware();
      if (!bound_middleware.is_object() ||
          bound_middleware.size() != declared_middleware.size())
        throw Refusal("middleware_mismatch",
                      "middleware bindings must match installation");
      for (const auto &item : declared_middleware.items()) {
        if (!bound_middleware.contains(item.key()))
          throw Refusal("middleware_mismatch", "missing middleware binding");
        const auto &bound = bound_middleware.at(item.key());
        if (bound.at("name") != item.value().at("name") ||
            bound.at("revision") != item.value().at("revision") ||
            !decimal(bound.at("realm")))
          throw Refusal("middleware_mismatch",
                        "middleware bindings must match installation");
      }
      middlewares = std::make_shared<MiddlewareRegistry>(declared_middleware);
      events = std::make_shared<EventRegistry>(declared_events);
      co_return Json{{"contract", service.contract()},
                     {"interface", descriptor},
                     {"dependency_interfaces", service.dependency_interfaces()},
                     {"dependencies", service.dependencies()},
                     {"composition", composition},
                     {"events", bound_events},
                     {"middleware", bound_middleware},
                     {"http", enable_http ? Json(url) : Json(nullptr)},
                     {"capabilities",
                      {{"multiplex", true},
                       {"reverse_calls", true},
                       {"effects", true},
                       {"effect_sequences", true},
                       {"scopes", true},
                       {"tasks", true},
                       {"events", true},
                       {"middleware", true},
                       {"composition", true},
                       {"metadata", true},
                       {"streams", false},
                       {"resources", true},
                       {"dependency_resources", true},
                       {"max_inflight", 16},
                       {"max_message", max_message}}}};
    }
    {
      std::lock_guard lock(state_mutex);
      if (!greeted || request.session != session ||
          request.activation != activation)
        throw Refusal("stale_session", "session or activation mismatch");
      remember(request);
      if (work(command)) {
        if (!active)
          throw Refusal("closed", "activation is not accepting calls");
        if (command != "resource_release") {
          if (inflight >= 16)
            throw Refusal("overloaded", "call capacity reached");
          ++inflight;
        }
      }
    }
    struct Guard {
      Host &host;
      bool counted;
      ~Guard() {
        if (!counted)
          return;
        bool drained;
        {
          std::lock_guard lock(host.state_mutex);
          drained = --host.inflight == 0 && !host.active;
        }
        if (drained)
          host.drained.trigger();
      }
    } guard{*this, work(command) && command != "resource_release"};
    Context ctx(
        dependencies,
        payload.is_object() ? payload.value("context", std::string()) : "",
        service.dependencies(), effects_enabled ? effects : nullptr,
        command == "effect_release" || command == "resource_release" ||
            command == "deactivate" || command == "sequence_undo" ||
            command == "sequence_release" || command == "middleware_release",
        sequences_enabled ? sequences : nullptr,
        scopes_enabled ? scopes : nullptr, tasks_enabled ? tasks : nullptr,
        tasks_enabled && command == "task_call"
            ? tasks->cancellation(payload.at("task").get<std::string>())
            : std::shared_ptr<corral::Event>(),
        events_enabled ? events : nullptr,
        middleware_enabled ? middlewares : nullptr,
        composition_enabled ? std::optional<Json>(composition) : std::nullopt);
    co_return co_await native::with_cleanup(
        [&]() -> Async<Payload> {
          if (command == "activate") {
            {
              std::lock_guard lock(state_mutex);
              if (activation_attempted)
                throw Refusal("protocol", "activation already attempted");
              activation_attempted = true;
            }
            (co_await service.activate(ctx));
            {
              std::lock_guard lock(state_mutex);
              active = true;
            }
            co_return Json::object();
          }
          if (command == "middleware_call" ||
              command == "middleware_terminal" ||
              command == "middleware_release") {
            {
              std::lock_guard lock(state_mutex);
              if (!middleware_enabled || !activation_attempted)
                throw Refusal("closed", "middleware unavailable");
            }
            if (command == "middleware_release") {
              middlewares->release(payload.at("handler").get<std::string>());
              co_return Json::object();
            }
            const auto call = payload.at("call").get<std::string>();
            if (command == "middleware_terminal")
              co_return (co_await middlewares->terminal(
                  payload.at("run").get<std::string>(), call,
                  request.payload.one(), ctx));
            const auto grant = payload.at("next").get<std::string>();
            if (grant.size() != 32 ||
                !std::all_of(grant.begin(), grant.end(), [](char c) {
                  return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
                }))
              throw Refusal("invalid_argument",
                            "invalid middleware continuation");
            Next next(dependencies, payload.at("context").get<std::string>(),
                      call, grant);
            co_return (co_await middlewares->invoke(
                payload.at("handler").get<std::string>(), call,
                request.payload.one(), ctx, next));
          }
          if (command == "event_call" || command == "event_release" ||
              command == "middleware_call" ||
              command == "middleware_terminal" ||
              command == "middleware_release") {
            {
              std::lock_guard lock(state_mutex);
              if (!events_enabled || !activation_attempted)
                throw Refusal("closed", "event unavailable");
            }
            const auto id = payload.at("listener").get<std::string>();
            if (command == "event_release") {
              events->release(id);
              co_return Json::object();
            }
            co_return (co_await events->invoke(
                id, payload.at("call").get<std::string>(),
                request.payload.one(), ctx));
          }
          if (command == "task_call" || command == "task_cancel") {
            {
              std::lock_guard lock(state_mutex);
              if (!tasks_enabled || !activation_attempted)
                throw Refusal("closed", "task unavailable");
            }
            const auto id = payload.at("task").get<std::string>();
            if (command == "task_cancel") {
              tasks->cancel(id);
              co_return Json::object();
            }
            co_return (co_await tasks->invoke(id, ctx));
          }
          if (command == "scope_call") {
            {
              std::lock_guard lock(state_mutex);
              if (!scopes_enabled || !activation_attempted)
                throw Refusal("closed", "scope callback unavailable");
            }
            co_return (co_await scopes->invoke(
                payload.at("scope").get<std::string>(),
                payload.at("run").get<std::string>(), ctx));
          }
          if (command == "sequence_next" || command == "sequence_undo" ||
              command == "sequence_release") {
            {
              std::lock_guard lock(state_mutex);
              if (!sequences_enabled || !activation_attempted)
                throw Refusal("closed", "sequence operation unavailable");
            }
            const auto id = payload.at("sequence").get<std::string>();
            if (command == "sequence_release") {
              sequences->release(id);
              co_return Json::object();
            }
            const auto step = payload.at("step").get<std::string>();
            if (command == "sequence_next")
              co_return (co_await sequences->advance(id, step, ctx));
            (co_await sequences->undo(id, step, ctx));
            co_return Json::object();
          }
          if (command == "effect_release") {
            {
              std::lock_guard lock(state_mutex);
              if (!effects_enabled || !activation_attempted)
                throw Refusal("closed", "effect cleanup unavailable");
            }
            (co_await effects->release(payload.at("effect").get<std::string>(),
                                       ctx));
            co_return Json::object();
          }
          if (command == "deactivate") {
            {
              std::unique_lock lock(state_mutex);
              active = false;
              if (inflight != 0) {
                lock.unlock();
                co_await drained;
                lock.lock();
              }
              if (middlewares && !middlewares->released())
                throw Refusal("middleware_remaining",
                              "middleware not released");
              if (events && !events->released())
                throw Refusal("listeners_remaining",
                              "event subscriptions not released");
              if (!tasks->idle())
                throw Refusal("tasks_running", "task callbacks still running");
              if (!scopes->idle())
                throw Refusal("scopes_running",
                              "scope callbacks still running");
              if (!sequences->released())
                throw Refusal("effects_remaining",
                              "sequences not confirmed released");
              if (!effects->released())
                throw Refusal("effects_remaining",
                              "caller effects not confirmed released");
              for (const auto &resource : resources) {
                if (!resource.second->released())
                  throw Refusal("resources_remaining",
                                "caller resources were not confirmed released");
              }
            }
            if (activation_attempted)
              (co_await service.deactivate(ctx));
            co_return Json::object();
          }
          if (!work(command))
            throw Refusal("unknown_command", command);
          if (command != "call")
            co_return (
                co_await resource_operation(command, request.payload, ctx));
          if (!payload.is_object() ||
              payload.size() != (payload.contains("context") ? 2u : 1u) ||
              !payload.at("method").is_string())
            throw Refusal("invalid_argument", "call requires method and args");
          const Json *signature;
          try {
            signature = &cordis::contract::operation(
                descriptor.at("methods"),
                payload.at("method").get<std::string>());
            if (!service.typed_dispatch()) cordis::contract::check_value(signature->at("args"),
                                          request.payload.one(), "method args", ctx.validation());
          } catch (const std::exception &e) {
            throw Refusal("invalid_argument", e.what());
          }
          auto result =
              (co_await checked_business(signature->at("errors"), [&] {
                return service.invoke(payload.at("method").get<std::string>(),
                                      request.payload.one(), ctx);
              }));
          if (!service.typed_dispatch()) cordis::contract::check_value(signature->at("result"), result,
                                        "method result", ctx.validation());
          co_return Payload(result);
        },
        [ctx] { return ctx.finish(); });
  }
  Async<Response> dispatch(Request request) {
    try {
      co_return response(
          request,
          Outcome{Outcome::Kind::success, co_await execute(request), {}, {}});
    } catch (const Refusal &error) {
      co_return failure(request, error.code, error.what(), "refused");
    } catch (const RpcError &error) {
      co_return failure(request, error.code, error.what());
    } catch (...) {
      dependencies->close();
      stop(1, true);
      throw;
    }
  }

public:
  explicit Host(Service &service, bool enable_http = true)
      : service(service),
        descriptor(cordis::contract::normalize(service.interface())),
        enable_http(enable_http) {}
  ~Host() { close(); }
  void stop(int status, bool discard_output = false) {
    exit_status.store(status);
    stdio.stop_reading();
    if (discard_output)
      stdio.abort();
  }
  void close() {
    if (closed)
      return;
    closed = true;
    dependencies->close();
    http_server.stop();
    runtime.post([this] { tasks->cancel_all(); });
    runtime.close();
  }
  Async<void> read() {
    while (auto frame = co_await stdio.read(greeted ? dependencies->validation : Validation::Audit)) {
      if (auto *reply = std::get_if<Response>(&*frame)) {
        dependencies->receive(*reply);
        continue;
      }
      auto request = std::get<Request>(std::move(*frame));
      const auto command = request.command;
      // Establish the immutable session policy before reading another frame.
      if (command == "hello") {
        output(co_await dispatch(std::move(request)));
        continue;
      }
      auto &lane =
          command == "middleware_call"       ? middleware_lane
          : command == "middleware_terminal" ? terminal_lane
          : command == "event_call"          ? event_lane
          : command == "task_call"           ? task_lane
          : command == "scope_call"          ? scope_lane
          : command == "sequence_next"       ? step_lane
          : (command == "effect_release" || command == "sequence_undo" ||
             command == "sequence_release" || command == "event_release" ||
             command == "middleware_release" || command == "resource_release" ||
             command == "task_cancel")
              ? cleanup_lane
              : ordinary_lane;
      if (!lane.post([this, request, command]() mutable -> Async<void> {
            try {
              const auto response = co_await dispatch(std::move(request));
              output(response);
              if (command == "deactivate")
                stop(response.outcome.kind == Outcome::Kind::success ? 0 : 1);
            } catch (...) {
              dependencies->close();
              stop(1, true);
            }
          }))
        output(failure(request, "overloaded", "call lane full", "refused"));
    }
  }
  int run() {
    std::signal(SIGPIPE, SIG_IGN);
    if (enable_http) {
      http_server.start();
      url = http_server.endpoint();
    }
    std::latch input_ended(1);
    runtime.submit([this, &input_ended]() -> Async<void> {
      try {
        co_await read();
      } catch (...) {
        if (exit_status.load() < 0)
          stop(1, true);
      }
      if (exit_status.load() < 0)
        stop(2, true);
      input_ended.count_down();
    });
    // Only the process entry thread waits here. All reads, writes, callbacks
    // and timers run on Asio; completion-driven shutdown needs no polling.
    input_ended.wait();
    close();
    return exit_status.load();
  }
};
} // namespace cordis::process
