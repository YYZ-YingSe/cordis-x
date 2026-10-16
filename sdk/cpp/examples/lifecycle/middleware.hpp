#pragma once
#include "common.hpp"
namespace example::lifecycle {
class Pipeline final : public ::lifecycle::PipelineServer {
  using Controls = ::lifecycle::PipelineControls;
  using Dependencies = ::lifecycle::PipelineDependencies;
  ::lifecycle::ListenerConfig config_;
  std::mutex mutex_;
  std::set<std::string> ids_;
  std::map<std::string, Effect> handlers_;
  std::optional<typed::Next<Number, Number>> saved_;

public:
  explicit Pipeline(::lifecycle::ListenerConfig config)
      : config_(std::move(config)) {}
  static std::shared_ptr<Pipeline> create(::lifecycle::ListenerConfig config) {
    return std::make_shared<Pipeline>(std::move(config));
  }
  Async<void> activate(Context ctx) override {
    if (config_.listen()) {
      ::lifecycle::HandlerSpec args;
      args.set_id("subscriber");
      args.set_behavior(::lifecycle::HandlerSpec::WRAP);
      co_await register_handler(ctx, std::move(args));
    }
    if (config_.fail_activate())
      throw RpcError("startup", "after registration");
  }
  Async<Unit> register_handler(Context ctx,
                               ::lifecycle::HandlerSpec args) override {
    const auto id = args.id();
    {
      std::lock_guard lock(mutex_);
      if (!ids_.insert(id).second)
        throw RpcError("duplicate_handler", id);
    }
    try {
      auto registration = co_await Controls::request().register_handler(
          ctx,
          [this, id, behavior = args.behavior()](
              Context current, Number args,
              typed::Next<Number, Number> next) -> Async<Number> {
            {
              std::lock_guard lock(mutex_);
              saved_ = next;
            }
            co_await log<Dependencies>(current, id + ".enter");
            co_await current.defer(id + ".resource", [id](Context cleanup) {
              return log<Dependencies>(std::move(cleanup), id + ".cleanup");
            });
            if (behavior == ::lifecycle::HandlerSpec::FAIL)
              throw RpcError("closed", "business failure");
            if (behavior == ::lifecycle::HandlerSpec::SHORT)
              co_return number(add(args.value(), 100));
            auto result = co_await next.run(number(add(args.value(), 1)));
            // A second call is a runtime refusal, never a successful string
            // result.
            if (behavior == ::lifecycle::HandlerSpec::DUPLICATE)
              co_await next.run(args);
            co_await log<Dependencies>(current, id + ".exit");
            co_return number(add(result.value(), 3));
          });
      std::lock_guard lock(mutex_);
      handlers_.emplace(id, std::move(registration));
    } catch (...) {
      std::lock_guard lock(mutex_);
      ids_.erase(id);
      throw;
    }
    co_return Unit{};
  }
  Async<Unit> release(Context, Text args) override {
    auto handler = [&] {
      std::lock_guard lock(mutex_);
      auto found = handlers_.find(args.value());
      if (found == handlers_.end())
        throw RpcError("unknown_handler", args.value());
      return found->second;
    }();
    co_await handler.dispose();
    co_return Unit{};
  }
  Async<Number> expired(Context, Number args) override {
    auto next = [&] {
      std::lock_guard lock(mutex_);
      if (!saved_)
        throw RpcError("no_next", "no middleware frame observed");
      return *saved_;
    }();
    co_return co_await next.run(std::move(args));
  }
  Async<Number> run(Context ctx, Number args) override {
    co_return co_await Controls::request().run(
        ctx, std::move(args),
        [](Context terminal, Number value) -> Async<Number> {
          co_await log<Dependencies>(terminal, "terminal.enter");
          co_await terminal.defer("terminal.resource", [](Context cleanup) {
            return log<Dependencies>(std::move(cleanup), "terminal.cleanup");
          });
          co_return number(add(value.value(), value.value()));
        });
  }
  Async<Number> identity(Context ctx, Number args) override {
    co_return co_await Controls::request().run(
        std::move(ctx), std::move(args),
        [](Context, Number value) -> Async<Number> { co_return value; });
  }
  Async<void> deactivate(Context ctx) override {
    co_await log<Dependencies>(std::move(ctx), "middleware.closed");
  }
};
using PipelineModel = ::lifecycle::PipelineModel<Pipeline>;
} // namespace example::lifecycle
