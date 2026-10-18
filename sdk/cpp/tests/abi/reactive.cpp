#include "json_fixture.hpp"
#include <atomic>
#include <condition_variable>
#include <cordis/adapters/abi/reactive.hpp>
#include <fstream>
#include <set>
#include <thread>
#ifndef CORDIS_ABI_CODE_VERSION
#define CORDIS_ABI_CODE_VERSION "v1"
#endif
using namespace cordis::abi;
using cordis::Async;
namespace {
// Test-only observation must not add guest TLS: macOS can retain TLS dylibs.
struct ObservedFrame {
  CordisAbiReactiveContext context{};
  uint64_t next = 0;
};
std::mutex frames_mutex;
std::map<uint64_t, ObservedFrame> frames;
Async<ObservedFrame> current_frame() {
  const auto caller = co_await detail::active_caller();
  std::lock_guard lock(frames_mutex);
  co_return frames.at(caller.grant);
}
std::atomic<bool> malformed_flow{false};
struct Watch {
  std::mutex mutex;
  std::set<std::string> paths;
  ~Watch() {
    for (const auto &path : paths)
      std::ofstream(path, std::ios::app) << "unload\n";
  }
} watch;
void trace(Value config, std::string event) {
  std::lock_guard<std::mutex> lock(watch.mutex);
  std::ofstream(config.at("trace").get<std::string>(), std::ios::app)
      << event << '\n';
}
struct Capture {
  Value config;
  std::string label;
  ~Capture() { trace(config, "released:" + label); }
};
struct State {
  std::mutex mutex;
  corral::Event terminal_entered;
  uint64_t next_id = 0;
  std::vector<Subscription> subscriptions;
  std::vector<Dispatch> dispatches;
  std::optional<Context> saved;
  std::optional<Next> next;
};
struct Fixture : fixture::JsonContextService {
  Value config;
  std::shared_ptr<State> state = std::make_shared<State>();
  explicit Fixture(Value config) : config(std::move(config)) {
    trace(this->config, "create");
  }
  ~Fixture() override { trace(config, "destroy"); }
  Async<void> activate(Context ctx) override {
    trace(config, "activate");
    if (config.value("startup", false)) {
      Value listener = {{"id", "startup"}, {"initializing", true}};
      co_await invoke_json(ctx, "on", std::move(listener));
      Value middleware = {{"id", "startup-middleware"}};
      co_await invoke_json(ctx, "use", std::move(middleware));
      (co_await invoke_json(ctx, "parallel", nullptr));
      throw Rejected("startup", "startup failed");
    }
    if (config.contains("initializing")) {
      Value listener = {{"id", "initializing"},
                        {"initializing", config["initializing"]}};
      co_await invoke_json(ctx, "on", std::move(listener));
      (co_await invoke_json(ctx, "parallel", nullptr));
    }

    co_return;
  }
  Async<void> deactivate(Context ctx) override {
    trace(config, "deactivate");
    (co_await fixture::invoke_json(ctx, "example.logger", "deactivate",
                                   CORDIS_ABI_CODE_VERSION));

    co_return;
  }
  Subscription subscription(size_t index) {
    std::lock_guard<std::mutex> lock(state->mutex);
    return state->subscriptions.at(index);
  }
  Async<Value> invoke_json(Context ctx, std::string method,
                           Value args) override {
    if (method == "version")
      co_return CORDIS_ABI_CODE_VERSION;
    if (method == "before") {
      auto config = this->config;
      (co_await ctx.defer("older", [config](Context) -> Async<void> {
        trace(config, "older");
        co_return;
      }));
      co_return nullptr;
    }
    if (method == "on" || method == "use") {
      auto config = this->config;
      auto state = this->state;
      auto label = args.value("id", "callback");
      auto capture = std::make_shared<Capture>();
      capture->config = config;
      capture->label = label;
      Subscription registration;
      if (method == "on") {
        registration = (co_await fixture::on_json(
            ctx, args.value("alias", "tick"),
            [config, state, args, label, capture](Context callback,
                                                  Value input) -> Async<Flow> {
              trace(config, "event:" + label);
              {
                std::lock_guard<std::mutex> lock(state->mutex);
                state->saved = callback;
              }
              Value event = {{"version", CORDIS_ABI_CODE_VERSION},
                             {"label", label},
                             {"input", input}};
              co_await fixture::invoke_json(callback, "example.logger", "event",
                                            std::move(event));
              if (args.value("hold", false))
                (co_await fixture::invoke_json(callback, "example.logger",
                                               "hold", label));
              if (args.value("nested", false) && input.get<int>() > 0)
                (co_await fixture::parallel_json(callback, "tick",
                                                 input.get<int>() - 1));
              if (args.value("self", false)) {
                Subscription own;
                {
                  std::lock_guard<std::mutex> lock(state->mutex);
                  own = state->subscriptions.front();
                }
                std::optional<Rejected> caught_2;
                try {
                  (co_await own.dispose());
                } catch (const Rejected &e) {
                  caught_2 = e;
                }
                if (caught_2) {
                  const auto &e = *caught_2;
                  (co_await fixture::invoke_json(callback, "example.logger",
                                                 "self_wait", e.code));
                }
              }
              if (args.value("late", false))
                (co_await callback.defer(
                    "event-late",
                    [config, args](Context cleanup) -> Async<void> {
                      trace(config, "late-cleanup");
                      (co_await fixture::invoke_json(cleanup, "example.logger",
                                                     "cleanup",
                                                     CORDIS_ABI_CODE_VERSION));
                      if (args.value("fail_cleanup", false))
                        throw Rejected("cleanup", "failed inverse");

                      co_return;
                    }));
              if (args.value("unknown", false))
                throw std::runtime_error("unknown event");
              if (args.value("reject", false))
                throw Rejected("business", "event rejected");
              co_return args.contains("break")
                  ? cordis::Break(fixture::body(args["break"]))
                  : cordis::Continue();
            },
            args.value("initializing", false)));
      } else {
        registration = (co_await fixture::middleware_json(
            ctx, args.value("alias", "chain"),
            [config, state, args, label, capture](Context callback, Value input,
                                                  Next next) -> Async<Value> {
              trace(config, "before:" + label);
              const auto frame = co_await current_frame();
              {
                std::lock_guard lock(state->mutex);
                state->saved = callback;
                state->next = next;
                state->next_id = frame.next;
              }
              (co_await fixture::invoke_json(callback, "example.logger",
                                             "middleware",
                                             CORDIS_ABI_CODE_VERSION));
              if (args.value("hold", false))
                (co_await fixture::invoke_json(callback, "example.logger",
                                               "hold", label));
              if (args.value("unknown", false))
                throw std::runtime_error("unknown middleware");
              if (args.value("reject", false))
                throw Rejected("business", "middleware rejected");
              if (args.contains("short"))
                co_return args["short"];
              if (args.value("detached", false)) {
                (co_await callback.spawn(
                    "admitted next",
                    [next, input, config](Context task) -> Async<void> {
                      auto result = (co_await fixture::next_json(
                          next, input.get<int>() + 1));
                      (co_await fixture::invoke_json(task, "example.logger",
                                                     "next-result", result));
                      trace(config, "detached-next-finished");

                      co_return;
                    }));
                auto wait = co_await corral::anyOf(
                    state->terminal_entered,
                    cordis::native::sleep(std::chrono::seconds(10)));
                if (!std::get<0>(wait))
                  throw std::runtime_error("terminal did not enter");
                trace(config, "early-handler-return");
                co_return 99;
              }
              auto output = (co_await fixture::next_json(
                  next, input.get<int>() + args.value("add", 1)));
              if (args.value("duplicate", false)) {
                std::optional<Rejected> caught_1;
                try {
                  (co_await fixture::next_json(next, input));
                } catch (const Rejected &e) {
                  caught_1 = e;
                }
                if (caught_1) {
                  const auto &e = *caught_1;
                  (co_await fixture::invoke_json(callback, "example.logger",
                                                 "duplicate", e.code));
                }
              }
              trace(config, "after:" + label);
              co_return output.get<int>() * args.value("multiply", 1);
            }));
      }
      std::lock_guard<std::mutex> lock(state->mutex);
      state->subscriptions.push_back(registration);
      co_return state->subscriptions.size() - 1;
    }
    if (method == "dispose") {
      (co_await subscription(args.get<size_t>()).dispose());
      trace(config, "disposed");
      co_return nullptr;
    }
    if (method == "parallel")
      co_return (co_await fixture::parallel_json(ctx, "tick", args));
    if (method == "serial")
      co_return (co_await fixture::serial_json(ctx, "tick", args));
    if (method == "emit") {
      auto dispatch = (co_await fixture::emit_json(
          ctx, "tick", args.value("input", Value(nullptr)),
          args.value("serial", false)));
      std::lock_guard<std::mutex> lock(state->mutex);
      state->dispatches.push_back(dispatch);
      co_return state->dispatches.size() - 1;
    }
    if (method == "join" || method == "status") {
      Dispatch dispatch;
      {
        std::lock_guard<std::mutex> lock(state->mutex);
        dispatch = state->dispatches.at(args.get<size_t>());
      }
      co_return method == "join" ? fixture::flows_json(co_await dispatch.join())
                                 : (co_await dispatch.snapshot());
    }
    if (method == "expired") {
      std::optional<Context> saved;
      {
        std::lock_guard<std::mutex> lock(state->mutex);
        saved = state->saved;
      }
      co_return (co_await fixture::parallel_json(*saved, "tick", nullptr));
    }
    if (method == "saved-next") {
      std::optional<Next> next;
      {
        std::lock_guard<std::mutex> lock(state->mutex);
        next = state->next;
      }
      co_return (co_await fixture::next_json(*next, args));
    }
    if (method == "foreign-next") {
      uint64_t next_id;
      {
        std::lock_guard<std::mutex> lock(state->mutex);
        next_id = state->next_id;
      }
      const auto raw = (co_await current_frame()).context;
      co_return fixture::json(
          (co_await detail::reactive_call(raw.base.base, raw.reactive,
                                          CORDIS_ABI_NEXT_RUN, next_id, 0, {},
                                          Payload(fixture::body(args))))
              .first.one());
    }
    if (method == "run") {
      auto config = this->config;
      auto state = this->state;
      co_return (co_await fixture::run_json(
          ctx, "chain", args,
          [config, state](Context terminal, Value input) -> Async<Value> {
            trace(config, "terminal");
            state->terminal_entered.trigger();
            if (config.value("hold_terminal", false))
              (co_await fixture::invoke_json(terminal, "example.logger", "hold",
                                             input));
            (co_await terminal.defer(
                "terminal-late", [config](Context cleanup) -> Async<void> {
                  trace(config, "terminal-cleanup");
                  (co_await fixture::invoke_json(cleanup, "example.logger",
                                                 "cleanup",
                                                 CORDIS_ABI_CODE_VERSION));

                  co_return;
                }));
            if (config.value("unknown_terminal", false))
              throw std::runtime_error("unknown terminal");
            if (config.value("reject_terminal", false))
              throw Rejected("terminal", "terminal rejected");
            co_return input;
          }));
    }
    if (method == "echo")
      co_return args;
    throw Rejected("method", "unknown method");
  }
};
struct Model {
  static cordis::Value interface() {
    return cordis::contract::value_interface(true);
  }
  static std::string contract_name() { return "example.context"; }
  static std::string contract_revision() { return "1"; }
  static Value dependencies() { return {{"example.logger", "1"}}; }
  static Value dependency_interfaces() {
    return {{"example.logger", cordis::contract::value_interface(true)}};
  }
  static Value bindings() {
    return {
        {"events", {{"tick", {{"name", "example.tick"}, {"revision", "1"}}}}},
        {"middleware",
         {{"chain", {{"name", "example.chain"}, {"revision", "1"}}}}}};
  }
  static std::shared_ptr<ContextService> create(Value config) {
    {
      std::lock_guard<std::mutex> lock(watch.mutex);
      watch.paths.insert(config.at("trace").get<std::string>());
    }
    malformed_flow.store(config.value("malformed_flow", false));
    return std::make_shared<Fixture>(config);
  }
};
} // namespace
extern "C" CORDIS_ABI_EXPORT uint32_t cordis_abi_v1(uint32_t version,
                                                    CordisAbiApi *out,
                                                    size_t size) {
  return ContextHost<Model>::entry(version, out, size);
}
extern "C" CORDIS_ABI_EXPORT uint32_t
cordis_abi_context_v1(uint32_t version, CordisAbiContextApi *out, size_t size) {
  return ContextHost<Model>::context_entry(version, out, size);
}
extern "C" CORDIS_ABI_EXPORT uint32_t
cordis_abi_managed_v1(uint32_t version, CordisAbiManagedApi *out, size_t size) {
  return ContextHost<Model>::managed_entry(version, out, size);
}
namespace {
decltype(CordisAbiReactiveApi{}.dispatch) original_dispatch = nullptr;
void traced_dispatch(uint64_t instance, uint32_t operation, uint64_t target,
                     CordisAbiReactiveContext raw, CordisAbiSlice method,
                     const CordisAbiBuffer *args,
                     CordisAbiCompletion done) noexcept {
  struct Pending {
    uint64_t grant;
    uint32_t operation;
    CordisAbiCompletion done;
    static void finish(void *data, CordisAbiResult result) noexcept {
      std::unique_ptr<Pending> pending(static_cast<Pending *>(data));
      if (pending->operation == CORDIS_ABI_EVENT_CALL &&
          malformed_flow.load()) {
        detail::release(&result);
        result = detail::protect([] {
          return detail::result(Value{{"kind", "continue"}, {"extra", true}});
        });
      }
      {
        std::lock_guard lock(frames_mutex);
        frames.erase(pending->grant);
      }
      detail::complete(pending->done, result);
    }
  };
  try {
    auto pending = std::make_unique<Pending>(
        Pending{raw.base.base.grant, operation, done});
    auto next = operation == CORDIS_ABI_MIDDLEWARE_CALL
                    ? detail::read(args).control.at("next").get<uint64_t>()
                    : 0;
    {
      std::lock_guard lock(frames_mutex);
      frames.emplace(pending->grant, ObservedFrame{raw, next});
    }
    CordisAbiCompletion completion{pending.get(), Pending::finish};
    pending.release();
    original_dispatch(instance, operation, target, raw, method, args,
                      completion);
  } catch (...) {
    {
      std::lock_guard lock(frames_mutex);
      frames.erase(raw.base.base.grant);
    }
    detail::complete(done, detail::fallback());
  }
}

} // namespace
extern "C" CORDIS_ABI_EXPORT uint32_t cordis_abi_reactive_v1(
    uint32_t version, CordisAbiReactiveApi *out, size_t size) {
  auto result = ContextHost<Model>::reactive_entry(version, out, size);
  if (!result) {
    original_dispatch = out->dispatch;
    out->dispatch = traced_dispatch;
  }
  return result;
}
