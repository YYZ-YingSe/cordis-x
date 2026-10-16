#pragma once
#include "context.hpp"

namespace cordis::process {
class Next {
  struct Frame {
    std::mutex mutex;
    corral::Event drained;
    bool open = true, used = false, running = false;
    std::shared_ptr<DependencyChannel> channel;
    std::string context, call, grant;
  };
  std::shared_ptr<Frame> frame;

public:
  Next(std::shared_ptr<DependencyChannel> channel, std::string context,
       std::string call, std::string grant)
      : frame(std::make_shared<Frame>()) {
    frame->channel = std::move(channel);
    frame->context = std::move(context);
    frame->call = std::move(call);
    frame->grant = std::move(grant);
  }
  Async<Message> run(Message args) const {
    return cordis::detail::invoke_owned(
        [*this](Message args) -> Async<Message> {
          {
            std::lock_guard lock(frame->mutex);
            if (!frame->open)
              throw Refusal("expired_next", "middleware frame ended");
            if (frame->used)
              throw Refusal("duplicate_next", "next already admitted");
            frame->used = true;
            frame->running = true;
          }
          struct Guard {
            std::shared_ptr<Frame> frame;
            ~Guard() {
              {
                std::lock_guard lock(frame->mutex);
                frame->running = false;
              }
              frame->drained.trigger();
            }
          } guard{frame};
          auto value = co_await frame->channel->request_payload(
              "middleware_next", Payload(Json{{"context", frame->context},
                                              {"call", frame->call},
                                              {"grant", frame->grant}},
                                         args));
          co_return value.one();
        },
        std::move(args));
  }
  void close() const {
    std::lock_guard lock(frame->mutex);
    frame->open = false;
  }
  Async<void> finish() const {
    return cordis::detail::invoke_owned([*this]() -> Async<void> {
      std::unique_lock lock(frame->mutex);
      frame->open = false;
      if (!frame->running)
        co_return;
      lock.unlock();
      co_await frame->drained;
    });
  }
};
using MiddlewareCallback =
    std::function<Async<Message>(Message, Context, Next)>;
using TerminalCallback = std::function<Async<Message>(Message, Context)>;
class MiddlewareRegistration {
  std::shared_ptr<DependencyChannel> channel;
  std::string handler, grant;

public:
  MiddlewareRegistration(std::shared_ptr<DependencyChannel> channel,
                         std::string handler, std::string grant)
      : channel(std::move(channel)), handler(std::move(handler)),
        grant(std::move(grant)) {}
  Async<void> dispose() const {
    return cordis::detail::invoke_owned([*this]() -> Async<void> {
      (co_await channel->request("middleware_dispose",
                                 {{"handler", handler}, {"grant", grant}}));
    });
  }
};
class MiddlewareRegistry {
  struct Entry {
    MiddlewareCallback callback;
    std::size_t running = 0;
  };
  struct Terminal {
    TerminalCallback callback;
    bool used = false;
  };
  std::mutex mutex;
  std::map<std::string, std::shared_ptr<Entry>> handlers;
  std::map<std::string, Terminal> runs;
  std::set<std::string> seen;
  std::uint64_t next_handler = 0, next_run = 0;
  Json declarations;
  void check(std::string alias) const {
    if (!declarations.contains(alias))
      throw Refusal("undeclared_middleware", alias);
  }
  void admit(std::string call) {
    if (call.size() < 2 || call.size() > 21 || call[0] != 'w' ||
        !std::all_of(call.begin() + 1, call.end(),
                     [](char c) { return c >= '0' && c <= '9'; }) ||
        seen.count(call))
      throw Refusal("closed", "middleware callback unavailable");
    if (seen.size() >= 4096)
      throw Refusal("session_exhausted", "middleware callback history full");
    seen.insert(call);
  }
  template <class F> static Async<Payload> outcome(F callback) {
    co_return Payload(co_await callback());
  }

public:
  explicit MiddlewareRegistry(Json declarations)
      : declarations(std::move(declarations)) {}
  Async<MiddlewareRegistration> add(std::shared_ptr<DependencyChannel> channel,
                                    std::string context, std::string middleware,
                                    MiddlewareCallback callback) {
    check(middleware);
    std::string id;
    {
      std::lock_guard lock(mutex);
      if (next_handler >= 4096)
        throw Refusal("middleware_exhausted", "handler history full");
      id = "m" + std::to_string(++next_handler);
      handlers.emplace(id,
                       std::make_shared<Entry>(Entry{std::move(callback), 0}));
    }
    auto reply = (co_await channel->request(
        "middleware_register",
        {{"context", context}, {"middleware", middleware}, {"handler", id}}));
    const auto grant = reply.at("grant").get<std::string>();
    if (reply.at("handler") != id || grant.size() != 32 ||
        !std::all_of(grant.begin(), grant.end(), [](char c) {
          return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
        }))
      throw Refusal("protocol", "invalid middleware acknowledgement");
    co_return MiddlewareRegistration(channel, id, grant);
  }
  Async<Message> run(std::shared_ptr<DependencyChannel> channel,
                     std::string context, std::string middleware, Message args,
                     TerminalCallback callback) {
    check(middleware);
    std::string id;
    {
      std::lock_guard lock(mutex);
      if (next_run >= 4096)
        throw Refusal("middleware_exhausted", "run history full");
      id = "q" + std::to_string(++next_run);
      runs.emplace(id, Terminal{std::move(callback), false});
    }
    struct Guard {
      MiddlewareRegistry &registry;
      std::string id;
      bool known = false;
      ~Guard() {
        if (!known)
          return;
        TerminalCallback removed;
        {
          std::lock_guard lock(registry.mutex);
          auto found = registry.runs.find(id);
          removed.swap(found->second.callback);
          registry.runs.erase(found);
        }
      }
    } guard{*this, id};
    try {
      auto value = co_await channel->request_payload(
          "middleware_run", Payload(Json{{"context", context},
                                         {"middleware", middleware},
                                         {"run", id}},
                                    args));
      auto message = value.one();
      guard.known = true;
      co_return message;
    } catch (const RpcError &) {
      guard.known = true;
      throw;
    }
  }
  Async<Payload> invoke(std::string id, std::string call, Message args,
                        Context ctx, Next next) {
    std::shared_ptr<Entry> entry;
    MiddlewareCallback callback;
    {
      std::lock_guard lock(mutex);
      auto found = handlers.find(id);
      if (found == handlers.end() || !found->second->callback)
        throw Refusal("closed", "middleware handler unavailable");
      admit(call);
      entry = found->second;
      ++entry->running;
      callback = entry->callback;
    }
    struct Guard {
      MiddlewareRegistry &registry;
      std::shared_ptr<Entry> entry;
      Context ctx;
      Next next;
      ~Guard() {
        next.close();
        std::lock_guard lock(registry.mutex);
        --entry->running;
      }
    } guard{*this, entry, ctx, next};
    co_return co_await outcome([&] {
      return native::with_cleanup([&] { return callback(args, ctx, next); },
                                  [ctx, next]() -> Async<void> {
                                    next.close();
                                    co_await ctx.finish();
                                    co_await next.finish();
                                  });
    });
  }
  Async<Payload> terminal(std::string id, std::string call, Message args,
                          Context ctx) {
    TerminalCallback callback;
    {
      std::lock_guard lock(mutex);
      auto found = runs.find(id);
      if (found == runs.end() || found->second.used)
        throw Refusal("closed", "terminal unavailable");
      admit(call);
      found->second.used = true;
      callback = found->second.callback;
    }
    co_return co_await outcome([&] {
      return native::with_cleanup([&] { return callback(args, ctx); },
                                  [ctx] { return ctx.finish(); });
    });
  }
  void release(std::string id) {
    MiddlewareCallback removed;
    {
      std::lock_guard lock(mutex);
      auto found = handlers.find(id);
      if (found == handlers.end())
        throw Refusal("closed", "unknown middleware handler");
      if (found->second->running)
        throw Refusal("middleware_running", "middleware has active callbacks");
      removed.swap(found->second->callback);
    }
  }
  bool released() {
    std::lock_guard lock(mutex);
    return runs.empty() &&
           std::all_of(handlers.begin(), handlers.end(), [](const auto &e) {
             return !e.second->callback && !e.second->running;
           });
  }
};
inline Async<MiddlewareRegistration>
Context::middleware(std::string alias, MiddlewareCallback callback) const {
  return cordis::detail::invoke_owned(
      [*this](std::string alias,
              MiddlewareCallback callback) -> Async<MiddlewareRegistration> {
        {
          std::lock_guard lock(frame->mutex);
          if (!frame->open || frame->token.empty() || frame->cleanup)
            throw Refusal("closed", "context cannot register middleware");
          if (!frame->middlewares)
            throw Refusal("unsupported_middleware",
                          "middleware capability not negotiated");
          ++frame->calls;
        }
        Guard guard{frame};
        co_return (co_await frame->middlewares->add(
            frame->channel, frame->token, alias, std::move(callback)));
      },
      std::move(alias), std::move(callback));
}
inline Async<Message> Context::waterfall(std::string alias, Message args,
                                         TerminalCallback terminal) const {
  return cordis::detail::invoke_owned(
      [*this](std::string alias, Message args,
              TerminalCallback terminal) -> Async<Message> {
        {
          std::lock_guard lock(frame->mutex);
          if (!frame->open || frame->token.empty())
            throw Refusal("closed", "invocation context expired");
          if (!frame->middlewares)
            throw Refusal("unsupported_middleware",
                          "middleware capability not negotiated");
          ++frame->calls;
        }
        Guard guard{frame};
        if (!terminal)
          terminal = [](Message value, Context) -> Async<Message> {
            co_return value;
          };
        co_return (co_await frame->middlewares->run(
            frame->channel, frame->token, alias, args, std::move(terminal)));
      },
      std::move(alias), std::move(args), std::move(terminal));
}
} // namespace cordis::process
