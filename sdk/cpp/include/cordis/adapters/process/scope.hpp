#pragma once
#include "context.hpp"

namespace cordis::process {
using ScopeCallback = std::function<Async<Json>(Context)>;

class Scope {
  std::shared_ptr<DependencyChannel> channel;
  std::shared_ptr<ScopeRegistry> registry;
  std::string id, grant;
  Async<Json> request(std::string command) const {
    return cordis::detail::invoke_owned(
        [*this](std::string command) -> Async<Json> {
          Json args = {{"scope", id}, {"grant", grant}};
          co_return co_await channel->request(command, std::move(args));
        },
        std::move(command));
  }

public:
  Scope(std::shared_ptr<DependencyChannel> channel,
        std::shared_ptr<ScopeRegistry> registry, std::string id,
        std::string grant)
      : channel(std::move(channel)), registry(std::move(registry)),
        id(std::move(id)), grant(std::move(grant)) {}
  Async<void> ready() const {
    return cordis::detail::invoke_owned(
        [*this]() -> Async<void> { (co_await request("scope_ready")); });
  }
  Async<void> dispose() const {
    return cordis::detail::invoke_owned(
        [*this]() -> Async<void> { (co_await request("scope_dispose")); });
  }
  Async<Json> snapshot() const {
    return cordis::detail::invoke_owned([*this]() -> Async<Json> {
      co_return (co_await request("scope_status"));
    });
  }
  Async<Json> run(ScopeCallback callback) const;
};

class ScopeRegistry : public std::enable_shared_from_this<ScopeRegistry> {
  struct Run {
    std::string scope;
    ScopeCallback callback;
    bool started = false, running = false;
  };
  std::mutex mutex;
  std::uint64_t next_scope = 0, next_run = 0;
  std::map<std::string, std::shared_ptr<Run>> runs;

public:
  Async<Scope> add(std::shared_ptr<DependencyChannel> channel,
                   std::string context, std::string label) {
    if (label.size() > 1024)
      throw Refusal("invalid_argument", "scope label too long");
    std::string id;
    {
      std::lock_guard lock(mutex);
      if (next_scope >= 256)
        throw Refusal("scopes_exhausted", "scope history full");
      id = "c" + std::to_string(++next_scope);
    }
    Json args = {{"context", context}, {"scope", id}, {"label", label}};
    const auto reply =
        co_await channel->request("scope_register", std::move(args));
    const auto grant = reply.at("grant").get<std::string>();
    if (reply.at("scope") != id || grant.size() != 32 ||
        !std::all_of(grant.begin(), grant.end(), [](char c) {
          return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
        }))
      throw Refusal("protocol", "invalid scope registration");
    co_return Scope(channel, shared_from_this(), id, grant);
  }
  Async<Json> run(std::shared_ptr<DependencyChannel> channel, std::string scope,
                  std::string grant, ScopeCallback callback) {
    auto entry = std::make_shared<Run>();
    entry->scope = scope;
    entry->callback = std::move(callback);
    std::string run;
    {
      std::lock_guard lock(mutex);
      if (next_run >= 4096)
        throw Refusal("runs_exhausted", "scope run history full");
      run = "u" + std::to_string(++next_run);
      runs.emplace(run, entry);
    }
    struct Guard {
      ScopeRegistry &registry;
      std::shared_ptr<Run> entry;
      ~Guard() {
        std::lock_guard lock(registry.mutex);
        entry->callback = {};
      }
    } guard{*this, entry};
    Json args = {{"scope", scope}, {"grant", grant}, {"run", run}};
    co_return co_await channel->request("scope_run", std::move(args));
  }
  Async<Json> invoke(std::string scope, std::string run, Context ctx) {
    std::shared_ptr<Run> entry;
    ScopeCallback callback;
    {
      std::lock_guard lock(mutex);
      const auto found = runs.find(run);
      if (found == runs.end() || found->second->scope != scope ||
          found->second->started || !found->second->callback)
        throw Refusal("closed", "scope callback unavailable");
      entry = found->second;
      entry->started = entry->running = true;
      callback = entry->callback;
    }
    struct Guard {
      ScopeRegistry &registry;
      std::shared_ptr<Run> entry;
      Context ctx;
      ~Guard() {
        std::lock_guard lock(registry.mutex);
        entry->callback = {};
        entry->running = false;
      }
    } guard{*this, entry, ctx};
    try {
      auto value = (co_await native::with_cleanup(
          [&] { return callback(ctx); }, [ctx] { return ctx.finish(); }));
      validate_control(value);
      co_return Json{{"kind", "success"}, {"value", std::move(value)}};
    } catch (const RpcError &error) {
      co_return Json{
          {"kind",
           dynamic_cast<const Refusal *>(&error) ? "refused" : "failure"},
          {"error", {{"code", error.code}, {"message", error.what()}}}};
    } catch (const std::exception &error) {
      co_return Json{
          {"kind", "failure"},
          {"error", {{"code", "scope_failed"}, {"message", error.what()}}}};
    } catch (...) {
      co_return Json{
          {"kind", "failure"},
          {"error",
           {{"code", "scope_failed"}, {"message", "non-standard exception"}}}};
    }
  }
  bool idle() {
    std::lock_guard lock(mutex);
    return std::all_of(runs.begin(), runs.end(),
                       [](const auto &r) { return !r.second->running; });
  }
};
inline Async<Json> Scope::run(ScopeCallback callback) const {
  return cordis::detail::invoke_owned(
      [*this](ScopeCallback callback) -> Async<Json> {
        co_return co_await registry->run(channel, id, grant,
                                         std::move(callback));
      },
      std::move(callback));
}
inline Async<Scope> Context::scope(std::string label) const {
  return cordis::detail::invoke_owned(
      [*this](std::string label) -> Async<Scope> {
        {
          std::lock_guard lock(frame->mutex);
          if (!frame->open || frame->token.empty() || frame->cleanup)
            throw Refusal("closed", "context cannot create scopes");
          if (!frame->scopes)
            throw Refusal("unsupported_scopes",
                          "scope capability not negotiated");
          ++frame->calls;
        }
        Guard guard{frame};
        co_return (
            co_await frame->scopes->add(frame->channel, frame->token, label));
      },
      std::move(label));
}
} // namespace cordis::process
