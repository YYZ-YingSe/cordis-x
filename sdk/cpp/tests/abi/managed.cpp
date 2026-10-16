#include "json_fixture.hpp"
#include <cordis/adapters/abi/managed.hpp>
#include <fstream>
#include <set>

#ifndef CORDIS_ABI_CODE_VERSION
#define CORDIS_ABI_CODE_VERSION "v1"
#endif
using namespace cordis::abi;
using cordis::Async;
namespace {
struct Watch {
  std::mutex mutex;
  std::set<std::string> paths;
  ~Watch() {
    for (auto &path : paths)
      std::ofstream(path, std::ios::app) << "unload\n";
  }
} watch;
void trace(Value config, std::string event) {
  std::lock_guard<std::mutex> lock(watch.mutex);
  std::ofstream(config.at("trace").get<std::string>(), std::ios::app)
      << event << '\n';
}
struct State {
  std::mutex mutex;
  std::optional<Sequence> sequence;
  std::optional<Scope> scope;
  std::vector<Task> tasks;
  std::optional<Context> saved;
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
      (co_await invoke_json(ctx, "scope-create", nullptr));
      (co_await invoke_json(ctx, "scope-run", "startup"));
      (co_await invoke_json(ctx, "task-start", nullptr));
      (co_await invoke_json(ctx, "sequence-start", nullptr));
      (co_await invoke_json(ctx, "sequence-ready", nullptr));
      throw Rejected("startup", "startup failed");
    }

    co_return;
  }
  Async<void> deactivate(Context ctx) override {
    trace(config, "deactivate");
    (co_await fixture::invoke_json(ctx, "example.logger", "deactivate",
                                   CORDIS_ABI_CODE_VERSION));

    co_return;
  }
  Sequence sequence() {
    std::lock_guard<std::mutex> lock(state->mutex);
    return state->sequence.value();
  }
  Scope scope() {
    std::lock_guard<std::mutex> lock(state->mutex);
    return state->scope.value();
  }
  Task task(Value args) {
    std::lock_guard<std::mutex> lock(state->mutex);
    return state->tasks.at(args.is_null() ? 0 : args.get<size_t>());
  }
  Async<Value> invoke_json(Context ctx, std::string method,
                           Value args) override {
    if (method == "metadata-for")
      co_return (co_await ctx.metadata(args.at("name"), args.at("revision")));
    if (method == "metadata")
      co_return (co_await ctx.metadata("example.context", "1"));
    if (method == "metadata-defer") {
      auto before = (co_await ctx.metadata("example.context", "1"));
      (co_await ctx.defer("metadata", [before](Context cleanup) -> Async<void> {
        auto after = (co_await cleanup.metadata("example.context", "1"));
        if (after != before)
          throw std::runtime_error("cleanup configuration changed");
        (co_await fixture::invoke_json(cleanup, "example.logger",
                                       "metadata-cleanup", after));

        co_return;
      }));
      co_return before;
    }
    if (method == "version")
      co_return CORDIS_ABI_CODE_VERSION;
    if (method == "sequence-start") {
      auto config = this->config;
      auto state = this->state;
      auto sequence = (co_await ctx.effect(
          "sequence",
          [config, state, number = 0](Context step) mutable -> Async<Step> {
            ++number;
            trace(config, "step:" + std::to_string(number));
            {
              std::lock_guard<std::mutex> lock(state->mutex);
              state->saved = step;
            }
            if (config.value("hold_step", false) && number == 1)
              (co_await fixture::invoke_json(step, "example.logger", "hold",
                                             number));
            if (config.value("self_sequence", false) && number == 1) {
              (co_await fixture::invoke_json(step, "example.logger", "hold",
                                             "self"));
              Sequence own;
              {
                std::lock_guard<std::mutex> lock(state->mutex);
                own = state->sequence.value();
              }
              std::optional<Rejected> caught_2;
              try {
                (co_await own.ready());
              } catch (const Rejected &e) {
                caught_2 = e;
              }
              if (caught_2) {
                const auto &e = *caught_2;
                (co_await fixture::invoke_json(step, "example.logger",
                                               "self_wait", e.code));
              }
            }
            if (config.value("unknown_step", false))
              throw std::runtime_error("unknown step");
            auto inverse =
                Inverse{"step " + std::to_string(number),
                        [config, number](Context cleanup) -> Async<void> {
                          trace(config, "undo:" + std::to_string(number));
                          (co_await fixture::invoke_json(
                              cleanup, "example.logger", "cleanup",
                              std::string(CORDIS_ABI_CODE_VERSION) +
                                  ":undo:" + std::to_string(number)));
                          if (config.value("fail_undo", false))
                            throw Rejected("undo", "inverse failed");

                          co_return;
                        }};
            bool last = number >= config.value("steps", 2);
            if (last && config.value("fail_step", false))
              co_return Step::failed(
                  Rejected("step", "failed after acquisition"), inverse);
            co_return last ? Step::complete(inverse) : Step::yielded(inverse);
          }));
      std::lock_guard<std::mutex> lock(state->mutex);
      state->sequence = sequence;
      co_return nullptr;
    }
    if (method == "sequence-ready")
      co_return (co_await sequence().ready());
    if (method == "sequence-dispose") {
      (co_await sequence().dispose());
      trace(config, "sequence-disposed");
      co_return nullptr;
    }
    if (method == "sequence-status")
      co_return (co_await sequence().snapshot());
    if (method == "scope-create") {
      auto scope = (co_await ctx.scope("connection"));
      std::lock_guard<std::mutex> lock(state->mutex);
      state->scope = scope;
      co_return nullptr;
    }
    if (method == "scope-ready") {
      (co_await scope().ready());
      co_return nullptr;
    }
    if (method == "scope-dispose") {
      (co_await scope().dispose());
      co_return nullptr;
    }
    if (method == "scope-status")
      co_return (co_await scope().snapshot());
    if (method == "scope-run") {
      auto state = this->state;
      auto config = this->config;
      co_return (co_await scope().run(
          [state, config, args](Context child) -> Async<Value> {
            (co_await fixture::invoke_json(child, "example.logger", "scoped",
                                           args));
            {
              std::lock_guard<std::mutex> lock(state->mutex);
              state->saved = child;
            }
            if (args == "hold")
              (co_await fixture::invoke_json(child, "example.logger", "hold",
                                             args));
            if (args == "nested") {
              auto nested = (co_await child.scope("nested"));
              (co_await nested.run([config](Context sub) -> Async<Value> {
                (co_await sub.defer(
                    "nested", [config](Context cleanup) -> Async<void> {
                      trace(config, "nested-cleanup");
                      (co_await fixture::invoke_json(cleanup, "example.logger",
                                                     "cleanup", "nested"));

                      co_return;
                    }));
                co_return nullptr;
              }));
            }
            if (args == "self") {
              Scope own;
              {
                std::lock_guard<std::mutex> lock(state->mutex);
                own = state->scope.value();
              }
              try {
                (co_await own.dispose());
              } catch (const Rejected &error) {
                co_return error.code;
              }
            }
            (co_await child.defer(
                "scope contribution", [config](Context cleanup) -> Async<void> {
                  trace(config, "scope-cleanup");
                  (co_await fixture::invoke_json(
                      cleanup, "example.logger", "cleanup",
                      std::string(CORDIS_ABI_CODE_VERSION) + ":scope"));
                  if (config.value("fail_scope", false))
                    throw Rejected("scope", "scope inverse failed");

                  co_return;
                }));
            if (args == "unknown")
              throw std::runtime_error("uncertain scope run");
            if (args == "reject")
              throw Rejected("business", "known scope error");
            co_return 42;
          }));
    }
    if (method == "task-start") {
      auto config = this->config;
      auto state = this->state;
      auto task = (co_await ctx.spawn(
          "worker", [config, state](Context task) -> Async<void> {
            trace(config, "task-start");
            (co_await fixture::invoke_json(task, "example.logger", "task-start",
                                           CORDIS_ABI_CODE_VERSION));
            {
              std::lock_guard<std::mutex> lock(state->mutex);
              state->saved = task;
            }
            (co_await task.defer(
                "task first", [config](Context cleanup) -> Async<void> {
                  trace(config, "task-first-cleanup");
                  (co_await fixture::invoke_json(cleanup, "example.logger",
                                                 "cleanup", "task-first"));

                  co_return;
                }));
            if (config.value("self_task", false)) {
              (co_await fixture::invoke_json(task, "example.logger", "hold",
                                             "self"));
              Task own;
              {
                std::lock_guard<std::mutex> lock(state->mutex);
                own = state->tasks.at(0);
              }
              std::optional<Rejected> caught_1;
              try {
                (co_await own.join());
              } catch (const Rejected &e) {
                caught_1 = e;
              }
              if (caught_1) {
                const auto &e = *caught_1;
                (co_await fixture::invoke_json(task, "example.logger",
                                               "self_wait", e.code));
              }
            }
            if (config.value("unknown_task", false))
              throw std::runtime_error("unknown task outcome");
            if (config.value("fail_task", false))
              throw Rejected("task", "task failed");
            if (!config.value("finite_task", false)) {
              (co_await task.cancelled_for(std::chrono::milliseconds(1)));
              if (!(co_await task.cancelled_for(std::chrono::seconds(15))))
                throw std::runtime_error("fixture stop timeout");
              (co_await task.cancelled());
            }
            trace(config, "task-stop");
            (co_await fixture::invoke_json(task, "example.logger", "task-stop",
                                           CORDIS_ABI_CODE_VERSION));
            (co_await task.defer(
                "task last", [config](Context cleanup) -> Async<void> {
                  trace(config, "task-last-cleanup");
                  (co_await fixture::invoke_json(cleanup, "example.logger",
                                                 "cleanup", "task-last"));

                  co_return;
                }));

            co_return;
          }));
      std::lock_guard<std::mutex> lock(state->mutex);
      auto id = state->tasks.size();
      state->tasks.push_back(task);
      co_return id;
    }
    if (method == "task-join") {
      (co_await task(args).join());
      co_return nullptr;
    }
    if (method == "task-status")
      co_return (co_await task(args).snapshot());
    if (method == "expired") {
      std::optional<Context> saved;
      {
        std::lock_guard<std::mutex> lock(state->mutex);
        saved = state->saved;
      }
      co_return (co_await fixture::invoke_json(*saved, "example.logger",
                                               "expired", nullptr));
    }
    if (method == "cancelled") {
      (co_await ctx.cancelled());
      co_return nullptr;
    }
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
  static std::shared_ptr<ContextService> create(Value config) {
    {
      std::lock_guard<std::mutex> lock(watch.mutex);
      watch.paths.insert(config.at("trace").get<std::string>());
    }
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
