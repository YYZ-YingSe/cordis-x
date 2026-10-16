#pragma once
#include "context.hpp"

namespace cordis::process {
using cordis::Break;
using cordis::Continue;
using EventCallback = std::function<Async<cordis::Flow>(Message, Context)>;
class EventDispatchError : public RpcError {
public:
  Json errors;
  explicit EventDispatchError(Json errors)
      : RpcError("event_failed", errors.dump()), errors(std::move(errors)) {}
};
class Subscription {
  std::shared_ptr<DependencyChannel> channel;
  std::string id, grant;

public:
  Subscription(std::shared_ptr<DependencyChannel> channel, std::string id,
               std::string grant)
      : channel(std::move(channel)), id(std::move(id)),
        grant(std::move(grant)) {}
  Async<void> dispose() const {
    return cordis::detail::invoke_owned([*this]() -> Async<void> {
      (co_await channel->request("event_dispose",
                                 {{"listener", id}, {"grant", grant}}));
    });
  }
};
class Dispatch {
  std::shared_ptr<DependencyChannel> channel;
  std::string id, grant;

public:
  Dispatch(std::shared_ptr<DependencyChannel> channel, std::string id,
           std::string grant)
      : channel(std::move(channel)), id(std::move(id)),
        grant(std::move(grant)) {}
  Async<std::vector<cordis::Flow>> join() const {
    return cordis::detail::invoke_owned(
        [*this]() -> Async<std::vector<cordis::Flow>> {
          const auto reply = co_await channel->request_payload(
              "event_join", Payload(Json{{"dispatch", id}, {"grant", grant}}));
          const auto &result = reply.control;
          if (result.value("kind", "") == "success" &&
              result.at("flows").is_array()) {
            std::vector<cordis::Flow> flows;
            std::size_t index = 0;
            for (const auto &tag : result.at("flows")) {
              if (tag == "continue")
                flows.push_back(cordis::Continue());
              else if (tag == "break" && index < reply.messages.size())
                flows.push_back(cordis::Break(reply.messages[index++]));
              else
                throw std::runtime_error("invalid event result");
            }
            if (index != reply.messages.size())
              throw std::runtime_error("unexpected event body");
            co_return flows;
          }
          if (result.value("kind", "") == "rejected")
            throw RpcError(result.at("error").at("code").get<std::string>(),
                           result.at("error").at("message").get<std::string>());
          if (result.value("kind", "") == "refused")
            throw Refusal(result.at("error").at("code").get<std::string>(),
                          result.at("error").at("message").get<std::string>());
          if (result.value("kind", "") == "unknown")
            throw std::runtime_error("event outcome unknown: " + result.dump());
          if (result.value("kind", "") == "failure" &&
              result.at("errors").is_array())
            throw EventDispatchError(result.at("errors"));
          throw Refusal("protocol", "invalid dispatch outcome");
        });
  }
  Async<Json> snapshot() const {
    return cordis::detail::invoke_owned([*this]() -> Async<Json> {
      co_return (co_await channel->request(
          "event_status", {{"dispatch", id}, {"grant", grant}}));
    });
  }
};
class EventRegistry {
  struct Entry {
    EventCallback callback;
    std::size_t running = 0;
  };
  std::mutex mutex;
  std::map<std::string, std::shared_ptr<Entry>> entries;
  std::set<std::string> seen;
  std::uint64_t next_listener = 0, next_dispatch = 0;
  Json declarations;
  void check(std::string event) const {
    if (!declarations.contains(event))
      throw Refusal("undeclared_event", event);
  }
  static std::string grant(Json reply, std::string key, std::string id) {
    const auto grant = reply.at("grant").get<std::string>();
    if (reply.at(key) != id || grant.size() != 32 ||
        !std::all_of(grant.begin(), grant.end(), [](char c) {
          return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
        }))
      throw Refusal("protocol", "invalid event acknowledgement");
    return grant;
  }

public:
  explicit EventRegistry(Json declarations)
      : declarations(std::move(declarations)) {}
  Async<Subscription> add(std::shared_ptr<DependencyChannel> channel,
                          std::string context, std::string event,
                          EventCallback callback) {
    check(event);
    std::string id;
    {
      std::lock_guard lock(mutex);
      if (next_listener >= 4096)
        throw Refusal("events_exhausted", "listener history full");
      id = "l" + std::to_string(++next_listener);
      entries.emplace(id,
                      std::make_shared<Entry>(Entry{std::move(callback), 0}));
    }
    auto reply = (co_await channel->request(
        "event_register",
        {{"context", context}, {"event", event}, {"listener", id}}));
    co_return Subscription(channel, id, grant(reply, "listener", id));
  }
  Async<Dispatch> emit(std::shared_ptr<DependencyChannel> channel,
                       std::string context, std::string event, Message args,
                       bool serial) {
    check(event);
    std::string id;
    {
      std::lock_guard lock(mutex);
      if (next_dispatch >= 4096)
        throw Refusal("events_exhausted", "dispatch history full");
      id = "d" + std::to_string(++next_dispatch);
    }
    auto received = co_await channel->request_payload(
        "event_emit", Payload(Json{{"context", context},
                                   {"event", event},
                                   {"mode", serial ? "serial" : "parallel"},
                                   {"dispatch", id}},
                              args));
    auto reply = received.control_only();
    co_return Dispatch(channel, id, grant(reply, "dispatch", id));
  }
  Async<Payload> invoke(std::string id, std::string call, Message args,
                        Context ctx) {
    std::shared_ptr<Entry> entry;
    EventCallback callback;
    {
      std::lock_guard lock(mutex);
      auto found = entries.find(id);
      if (found == entries.end() || !found->second->callback ||
          call.size() < 2 || call.size() > 21 || call[0] != 'v' ||
          !std::all_of(call.begin() + 1, call.end(),
                       [](char c) { return c >= '0' && c <= '9'; }) ||
          seen.count(call))
        throw Refusal("closed", "event callback unavailable");
      if (seen.size() >= 4096)
        throw Refusal("session_exhausted", "event callback history full");
      seen.insert(call);
      entry = found->second;
      ++entry->running;
      callback = entry->callback;
    }
    struct Guard {
      EventRegistry &registry;
      std::shared_ptr<Entry> entry;
      Context ctx;
      ~Guard() {
        std::lock_guard lock(registry.mutex);
        --entry->running;
      }
    } guard{*this, entry, ctx};
    auto flow = co_await native::with_cleanup(
        [&] { return callback(args, ctx); }, [ctx] { return ctx.finish(); });
    co_return flow_payload(flow);
  }
  void release(std::string id) {
    EventCallback removed;
    {
      std::lock_guard lock(mutex);
      auto found = entries.find(id);
      if (found == entries.end())
        throw Refusal("closed", "unknown listener");
      if (found->second->running)
        throw Refusal("listeners_running", "listener has active callbacks");
      // A moved-from std::function may still hold a callable. Swap with the
      // empty local to retire it reliably, then destroy captures unlocked.
      removed.swap(found->second->callback);
    }
  }
  bool released() {
    std::lock_guard lock(mutex);
    return std::all_of(entries.begin(), entries.end(), [](const auto &e) {
      return !e.second->callback && !e.second->running;
    });
  }
};
inline Async<Subscription> Context::on(std::string event,
                                       EventCallback callback) const {
  return cordis::detail::invoke_owned(
      [*this](std::string event,
              EventCallback callback) -> Async<Subscription> {
        {
          std::lock_guard lock(frame->mutex);
          if (!frame->open || frame->token.empty() || frame->cleanup)
            throw Refusal("closed", "context cannot subscribe");
          if (!frame->events)
            throw Refusal("unsupported_events",
                          "events capability not negotiated");
          ++frame->calls;
        }
        Guard guard{frame};
        co_return (co_await frame->events->add(frame->channel, frame->token,
                                               event, std::move(callback)));
      },
      std::move(event), std::move(callback));
}
inline Async<Dispatch> Context::emit(std::string event, Message args) const {
  return cordis::detail::invoke_owned(
      [*this](std::string event, Message args) -> Async<Dispatch> {
        {
          std::lock_guard lock(frame->mutex);
          if (!frame->open || frame->token.empty())
            throw Refusal("closed", "invocation context expired");
          if (!frame->events)
            throw Refusal("unsupported_events",
                          "events capability not negotiated");
          ++frame->calls;
        }
        Guard guard{frame};
        co_return (co_await frame->events->emit(frame->channel, frame->token,
                                                event, args, false));
      },
      std::move(event), std::move(args));
}
inline Async<std::vector<cordis::Flow>> Context::parallel(std::string event,
                                                          Message args) const {
  return cordis::detail::invoke_owned(
      [*this](std::string event,
              Message args) -> Async<std::vector<cordis::Flow>> {
        co_return (co_await dispatch_event(event, args, false));
      },
      std::move(event), std::move(args));
}
inline Async<cordis::Flow> Context::serial(std::string event,
                                           Message args) const {
  return cordis::detail::invoke_owned(
      [*this](std::string event, Message args) -> Async<cordis::Flow> {
        auto flows = co_await dispatch_event(event, args, true);
        if (flows.size() != 1)
          throw std::runtime_error("invalid serial flow count");
        co_return flows.front();
      },
      std::move(event), std::move(args));
}
inline Async<std::vector<cordis::Flow>>
Context::dispatch_event(std::string event, Message args, bool serial) const {
  return cordis::detail::invoke_owned(
      [*this](std::string event, Message args,
              bool serial) -> Async<std::vector<cordis::Flow>> {
        {
          std::lock_guard lock(frame->mutex);
          if (!frame->open || frame->token.empty())
            throw Refusal("closed", "invocation context expired");
          if (!frame->events)
            throw Refusal("unsupported_events",
                          "events capability not negotiated");
          ++frame->calls;
        }
        Guard guard{frame};
        auto dispatch = co_await frame->events->emit(
            frame->channel, frame->token, event, args, serial);
        co_return co_await dispatch.join();
      },
      std::move(event), std::move(args), std::move(serial));
}
} // namespace cordis::process
