#pragma once
#include "common.hpp"
namespace example::lifecycle {
class Composer final : public ::lifecycle::ComposerServer {
  using Controls = ::lifecycle::ComposerControls;
  ::lifecycle::CompositionConfig config_;
  std::mutex mutex_;
  std::set<std::string> names_;
  std::map<std::string, Child> children_;
  Child child(const std::string &name) {
    std::lock_guard lock(mutex_);
    auto found = children_.find(name);
    if (found == children_.end())
      throw RpcError("unknown_child", name);
    return found->second;
  }
  Async<Child> install_child(Context ctx, ::lifecycle::WorkerConfig args) {
    const auto name = args.name();
    {
      std::lock_guard lock(mutex_);
      if (!names_.insert(name).second)
        throw RpcError("duplicate_child", name);
    }
    try {
      auto value =
          co_await Controls::worker().install(std::move(ctx), std::move(args));
      std::lock_guard lock(mutex_);
      children_.emplace(name, value);
      co_return value;
    } catch (...) {
      std::lock_guard lock(mutex_);
      names_.erase(name);
      throw;
    }
  }

public:
  explicit Composer(::lifecycle::CompositionConfig config)
      : config_(std::move(config)) {}
  static std::shared_ptr<Composer>
  create(::lifecycle::CompositionConfig config) {
    return std::make_shared<Composer>(std::move(config));
  }
  Async<void> activate(Context ctx) override {
    ::lifecycle::Settings settings;
    settings.set_answer(42);
    if (config_.publication() != ::lifecycle::CompositionConfig::OMIT)
      co_await Controls::settings().provide(ctx, settings);
    if (config_.publication() == ::lifecycle::CompositionConfig::DUPLICATE) {
      settings.set_answer(43);
      co_await Controls::settings().provide(ctx, settings);
    }
    if (config_.fail_activate()) {
      ::lifecycle::WorkerConfig args;
      args.set_name("startup");
      co_await Controls::worker().install(ctx, std::move(args));
      throw RpcError("startup_failed", "after child installation");
    }
  }
  Async<::lifecycle::ChildStatus>
  install(Context ctx, ::lifecycle::WorkerConfig args) override {
    auto value = co_await install_child(std::move(ctx), std::move(args));
    co_return child_status(co_await value.ready());
  }
  Async<Unit> install_pending(Context ctx,
                              ::lifecycle::WorkerConfig args) override {
    co_await install_child(std::move(ctx), std::move(args));
    co_return Unit{};
  }
  Async<Unit> release(Context, Text args) override {
    co_await child(args.value()).dispose();
    co_return Unit{};
  }
  Async<::lifecycle::ChildStatus> ready(Context, Text args) override {
    co_return child_status(co_await child(args.value()).ready());
  }
  Async<::lifecycle::ChildStatus> settled(Context, Text args) override {
    co_return child_status(co_await child(args.value()).settled());
  }
  Async<::lifecycle::ChildStatus> status(Context, Text args) override {
    co_return child_status(co_await child(args.value()).status());
  }
};
class Application final : public ::lifecycle::ApplicationServer {
  using Controls = ::lifecycle::ApplicationControls;

public:
  static std::shared_ptr<Application> create(cordis::data::Unit) {
    return std::make_shared<Application>();
  }
  Async<void> activate(Context ctx) override {
    ::lifecycle::WorkerConfig args;
    args.set_name("application");
    auto worker = co_await Controls::worker().install(ctx, std::move(args));
    co_await worker.ready();
    ::lifecycle::ApplicationInfo info;
    info.set_name("C++ application");
    info.set_version(1);
    co_await Controls::info().provide(ctx, std::move(info));
  }
  Async<::lifecycle::ChildStatus>
  start(Context ctx, ::lifecycle::WorkerConfig args) override {
    auto worker =
        co_await Controls::worker().install(std::move(ctx), std::move(args));
    co_return child_status(co_await worker.ready());
  }
};
using ComposerModel = ::lifecycle::ComposerModel<Composer>;
using ApplicationModel = ::lifecycle::ApplicationModel<Application>;
} // namespace example::lifecycle
