#pragma once
#include "wire.hpp"
#include <algorithm>
#include <cordis/core/error.hpp>
#include <cordis/protocols/session/channel.hpp>
#include <cordis/runtime/native.hpp>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>

namespace cordis::process {
using ::cordis::RpcError;
using session::DependencyChannel;

class Effect {
  std::shared_ptr<DependencyChannel> channel;
  std::string effect, grant;

public:
  Effect(std::shared_ptr<DependencyChannel> channel, std::string effect,
         std::string grant)
      : channel(std::move(channel)), effect(std::move(effect)),
        grant(std::move(grant)) {}
  // Dropping a handle does not dispose; the Runtime still owns this
  // registration.
  Async<void> dispose() const {
    return cordis::detail::invoke_owned([*this]() -> Async<void> {
      Json args = {{"effect", effect}, {"grant", grant}};
      co_await channel->request("effect_dispose", std::move(args));
    });
  }
};
class Context;
struct Inverse {
  std::string label;
  std::function<Async<void>(Context)> cleanup;
};
struct EffectStep {
  bool done;
  std::optional<Inverse> inverse;
  std::optional<RpcError> error;
  static EffectStep yielded(std::optional<Inverse> inverse = {}) {
    return {false, std::move(inverse), {}};
  }
  static EffectStep complete(std::optional<Inverse> inverse = {}) {
    return {true, std::move(inverse), {}};
  }
  static EffectStep failed(RpcError error,
                           std::optional<Inverse> inverse = {}) {
    return {true, std::move(inverse), std::move(error)};
  }
};
class EffectSequence {
  std::shared_ptr<DependencyChannel> channel;
  std::string sequence, grant;
  Async<Json> request(std::string command) const {
    return cordis::detail::invoke_owned(
        [*this](std::string command) -> Async<Json> {
          Json args = {{"sequence", sequence}, {"grant", grant}};
          co_return co_await channel->request(command, std::move(args));
        },
        std::move(command));
  }

public:
  EffectSequence(std::shared_ptr<DependencyChannel> channel,
                 std::string sequence, std::string grant)
      : channel(std::move(channel)), sequence(std::move(sequence)),
        grant(std::move(grant)) {}
  Async<std::string> ready() const {
    return cordis::detail::invoke_owned([*this]() -> Async<std::string> {
      auto completion = (co_await request("sequence_ready"))
                            .at("completion")
                            .get<std::string>();
      if (completion != "completed" && completion != "stopped")
        throw Refusal("protocol", "invalid sequence completion");
      co_return completion;
    });
  }
  Async<void> dispose() const {
    return cordis::detail::invoke_owned(
        [*this]() -> Async<void> { (co_await request("sequence_dispose")); });
  }
  Async<Json> snapshot() const {
    return cordis::detail::invoke_owned([*this]() -> Async<Json> {
      co_return (co_await request("sequence_status"));
    });
  }
};
class SequenceRegistry;
class EffectRegistry;
class Scope;
class ScopeRegistry;
class Task;
class TaskRegistry;
class EventRegistry;
class Subscription;
class Dispatch;

// Copyable, but valid only for the callback that received it. No raw owner IDs.
class Next;
class MiddlewareRegistration;
class MiddlewareRegistry;
class Child;
class Context {
  struct Frame {
    std::mutex mutex;
    corral::Event drained;
    bool open = true;
    std::size_t calls = 0;
    std::shared_ptr<DependencyChannel> channel;
    std::string token;
    Json dependencies;
    std::optional<Json> composition;
    std::shared_ptr<EffectRegistry> effects;
    bool cleanup = false;
    std::shared_ptr<SequenceRegistry> sequences;
    std::shared_ptr<ScopeRegistry> scopes;
    std::shared_ptr<TaskRegistry> tasks;
    std::shared_ptr<corral::Event> cancellation;
    std::shared_ptr<EventRegistry> events;
    std::shared_ptr<MiddlewareRegistry> middlewares;
  };
  std::shared_ptr<Frame> frame;
  Async<std::vector<cordis::Flow>> dispatch_event(std::string, Message,
                                                  bool) const;
  struct Guard {
    std::shared_ptr<Frame> frame;
    ~Guard() {
      bool drained;
      {
        std::lock_guard lock(frame->mutex);
        drained = --frame->calls == 0 && !frame->open;
      }
      if (drained)
        frame->drained.trigger();
    }
  };

public:
  Validation validation() const { return frame->channel->validation; }
  class ResourceHandle {
    std::weak_ptr<DependencyChannel> channel;
    std::string id, grant;
    ResourceHandle(std::shared_ptr<DependencyChannel> channel, std::string id,
                   std::string grant)
        : channel(channel), id(std::move(id)), grant(std::move(grant)) {}
    friend class Context;
  };

  Context(std::shared_ptr<DependencyChannel> channel, std::string token,
          Json dependencies, std::shared_ptr<EffectRegistry> effects = {},
          bool cleanup = false,
          std::shared_ptr<SequenceRegistry> sequences = {},
          std::shared_ptr<ScopeRegistry> scopes = {},
          std::shared_ptr<TaskRegistry> tasks = {},
          std::shared_ptr<corral::Event> cancellation = {},
          std::shared_ptr<EventRegistry> events = {},
          std::shared_ptr<MiddlewareRegistry> middlewares = {},
          std::optional<Json> composition = {})
      : frame(std::make_shared<Frame>()) {
    frame->channel = std::move(channel);
    frame->token = std::move(token);
    frame->dependencies = std::move(dependencies);
    frame->effects = std::move(effects);
    frame->cleanup = cleanup;
    frame->sequences = std::move(sequences);
    frame->scopes = std::move(scopes);
    frame->tasks = std::move(tasks);
    frame->cancellation = std::move(cancellation);
    frame->events = std::move(events);
    frame->middlewares = std::move(middlewares);
    frame->composition = std::move(composition);
  }
  Async<ResourceHandle> acquire(std::string dependency, std::string kind,
                                Message args) const {
    return cordis::detail::invoke_owned(
        [*this](std::string dependency, std::string kind,
                Message args) -> Async<ResourceHandle> {
          {
            std::lock_guard lock(frame->mutex);
            if (!frame->open || frame->token.empty() || frame->cleanup)
              throw Refusal("closed",
                            "acquire requires a live ordinary Context");
            if (!frame->dependencies.contains(dependency))
              throw Refusal("undeclared_dependency", dependency);
            ++frame->calls;
          }
          Guard guard{frame};
          const auto id = frame->channel->resource_id();
          Payload payload(Json{{"context", frame->token},
                               {"dependency", dependency},
                               {"kind", kind},
                               {"resource", id}},
                          args);
          const auto received = co_await frame->channel->request_payload(
              "dependency_resource_acquire", std::move(payload));
          const auto reply = operation_outcome(received.control_only());
          if (!reply.is_object() || reply.size() != 2 ||
              !reply.contains("resource") || reply["resource"] != id ||
              !reply.contains("grant") || !reply["grant"].is_string())
            throw std::runtime_error(
                "invalid resource acquisition acknowledgement");
          auto grant = reply["grant"].get<std::string>();
          if (grant.size() != 32 ||
              !std::all_of(grant.begin(), grant.end(), [](char c) {
                return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
              }))
            throw std::runtime_error("invalid resource grant");
          co_return ResourceHandle(frame->channel, id, std::move(grant));
        },
        std::move(dependency), std::move(kind), std::move(args));
  }
  Async<Message> resource_invoke(ResourceHandle resource, std::string method,
                                 Message args) const {
    return cordis::detail::invoke_owned(
        [*this](ResourceHandle resource, std::string method,
                Message args) -> Async<Message> {
          co_return (co_await resource_request(resource, false, method, args))
              .one();
        },
        std::move(resource), std::move(method), std::move(args));
  }
  Async<void> resource_dispose(ResourceHandle resource) const {
    return cordis::detail::invoke_owned(
        [*this](ResourceHandle resource) -> Async<void> {
          if (!(co_await resource_request(resource, true, "", std::nullopt))
                   .control_only()
                   .is_null())
            throw std::runtime_error(
                "invalid resource disposal acknowledgement");
        },
        std::move(resource));
  }

private:
  static Json operation_outcome(Json reply) {
    if (reply.is_object() && reply.size() == 2) {
      if (reply.contains("status") && reply["status"] == "ok" &&
          reply.contains("value"))
        return reply["value"];
      if (reply.contains("status") &&
          (reply["status"] == "rejected" || reply["status"] == "refused") &&
          reply.contains("error") && reply["error"].is_object() &&
          reply["error"].size() == 2 && reply["error"].contains("code") &&
          reply["error"]["code"].is_string() &&
          reply["error"].contains("message") &&
          reply["error"]["message"].is_string()) {
        if (reply["status"] == "refused")
          throw Refusal(reply["error"]["code"].get<std::string>(),
                        reply["error"]["message"].get<std::string>());
        throw RpcError(reply["error"]["code"].get<std::string>(),
                       reply["error"]["message"].get<std::string>());
      }
    }
    // Unknown and malformed results never become a known business rejection.
    throw std::runtime_error("unknown operation outcome: " + reply.dump());
  }
  Async<Payload> resource_request(ResourceHandle resource, bool dispose,
                                  std::string method,
                                  std::optional<Message> args) const {
    return cordis::detail::invoke_owned(
        [*this](ResourceHandle resource, bool dispose, std::string method,
                std::optional<Message> args) -> Async<Payload> {
          {
            std::lock_guard lock(frame->mutex);
            if (!frame->open || frame->token.empty() ||
                (dispose && frame->cleanup))
              throw Refusal("closed",
                            "resource operation requires a live Context");
            if (resource.channel.lock() != frame->channel)
              throw Refusal("invalid_handle",
                            "resource belongs to another session");
            ++frame->calls;
          }
          Guard guard{frame};
          Json payload = {{"context", frame->token},
                          {"resource", resource.id},
                          {"grant", resource.grant}};
          if (!dispose) {
            payload["method"] = method;
          }
          Payload input(payload);
          if (args)
            input.messages.push_back(*args);
          auto reply = co_await frame->channel->request_payload(
              dispose          ? "dependency_resource_dispose"
              : frame->cleanup ? "cleanup_resource_call"
                               : "dependency_resource_call",
              std::move(input));
          reply.control = operation_outcome(reply.control);
          co_return reply;
        },
        std::move(resource), std::move(dispose), std::move(method),
        std::move(args));
  }

public:
  Async<Message> invoke(std::string dependency, std::string method,
                        Message args) const {
    return cordis::detail::invoke_owned(
        [*this](std::string dependency, std::string method,
                Message args) -> Async<Message> {
          {
            std::lock_guard lock(frame->mutex);
            if (!frame->open || frame->token.empty())
              throw Refusal("closed", "invocation context expired");
            if (!frame->dependencies.contains(dependency))
              throw Refusal("undeclared_dependency", dependency);
            ++frame->calls;
          }
          Guard guard{frame};
          co_return (co_await frame->channel->invoke(
              frame->token, dependency, method, args, frame->cleanup));
        },
        std::move(dependency), std::move(method), std::move(args));
  }
  Async<Json> metadata(std::string name, std::string revision) const {
    return cordis::detail::invoke_owned(
        [*this](std::string name, std::string revision) -> Async<Json> {
          {
            std::lock_guard lock(frame->mutex);
            if (!frame->open || frame->token.empty())
              throw Refusal("closed", "context expired");
            ++frame->calls;
          }
          Guard guard{frame};
          co_return (co_await frame->channel->metadata(
              frame->token, name, revision, frame->cleanup));
        },
        std::move(name), std::move(revision));
  }
  Async<Child> install(std::string factory, Message config) const;
  Async<void> provide(std::string service, Message value) const;
  Async<Effect> defer(std::string label,
                      std::function<Async<void>(Context)> cleanup) const;
  Async<EffectSequence>
  effect(std::string label,
         std::function<Async<EffectStep>(Context)> producer) const;
  Async<MiddlewareRegistration>
      middleware(std::string,
                 std::function<Async<Message>(Message, Context, Next)>) const;
  Async<Message> waterfall(
      std::string, Message,
      std::function<Async<Message>(Message, Context)> terminal = {}) const;
  Async<Subscription>
      on(std::string,
         std::function<Async<cordis::Flow>(Message, Context)>) const;
  Async<Dispatch> emit(std::string, Message) const;
  Async<std::vector<cordis::Flow>> parallel(std::string, Message) const;
  Async<cordis::Flow> serial(std::string, Message) const;
  Async<Scope> scope(std::string label) const;
  Async<Task> spawn(std::string label,
                    std::function<Async<void>(Context)> callback) const;
  std::shared_ptr<corral::Event> cancellation() const {
    std::lock_guard lock(frame->mutex);
    if (!frame->open || !frame->cancellation)
      throw Refusal("closed", "cancellation requires a live task context");
    return frame->cancellation;
  }
  Async<void> cancelled() const {
    return cordis::detail::invoke_owned([*this]() -> Async<void> {
      auto signal = cancellation();
      co_await *signal;
    });
  }
  Async<bool> cancelled_for(std::chrono::milliseconds duration) const {
    return cordis::detail::invoke_owned(
        [*this](std::chrono::milliseconds duration) -> Async<bool> {
          auto signal = cancellation();
          auto [cancelled, elapsed] =
              co_await corral::anyOf(signal->get(), native::sleep(duration));
          co_return cancelled.has_value();
        },
        std::move(duration));
  }
  Async<void> finish() const {
    return cordis::detail::invoke_owned([*this]() -> Async<void> {
      {
        std::lock_guard lock(frame->mutex);
        frame->open = false;
        if (frame->calls == 0)
          co_return;
      }
      co_await frame->drained;
    });
  }
};

// Callback storage only. The native Runtime decides ownership and cleanup
// order.
class EffectRegistry {
  struct Entry {
    std::function<Async<void>(Context)> cleanup;
    std::mutex mutex;
    corral::Event changed;
    bool started = false, finished = false;
    std::exception_ptr failure;
    Async<void> release(Context ctx) {
      std::unique_lock lock(mutex);
      if (started) {
        lock.unlock();
        co_await changed;
        if (failure)
          std::rethrow_exception(failure);
        co_return;
      }
      started = true;
      lock.unlock();
      std::exception_ptr error;
      try {
        (co_await cleanup(ctx));
      } catch (...) {
        error = std::current_exception();
      }
      co_await ctx.finish();
      lock.lock();
      failure = error;
      if (!failure)
        cleanup = {};
      finished = true;
      lock.unlock();
      changed.trigger();
      if (failure)
        std::rethrow_exception(failure);
    }
  };
  std::mutex mutex;
  std::map<std::string, std::shared_ptr<Entry>> entries;
  std::uint64_t next = 0;

public:
  Async<Effect> add(std::shared_ptr<DependencyChannel> channel,
                    std::string context, std::string label,
                    std::function<Async<void>(Context)> cleanup) {
    if (label.size() > 1024)
      throw Refusal("invalid_argument", "effect label too long");
    std::string id;
    auto entry = std::make_shared<Entry>();
    entry->cleanup = std::move(cleanup);
    {
      std::lock_guard lock(mutex);
      if (next >= 4096)
        throw Refusal("effects_exhausted", "effect history full");
      id = "e" + std::to_string(++next);
      entries.emplace(id, entry);
    }
    // Retain on any failed/unknown registration: deactivation must not claim it
    // was cleaned. A release can arrive before the registration
    // acknowledgement.
    Json args = {{"context", context}, {"effect", id}, {"label", label}};
    auto reply = co_await channel->request("effect_register", std::move(args));
    const auto grant = reply.at("grant").get<std::string>();
    if (reply.at("effect") != id || grant.size() != 32 ||
        !std::all_of(grant.begin(), grant.end(), [](char c) {
          return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
        }))
      throw Refusal("protocol", "invalid effect registration acknowledgement");
    co_return Effect(channel, id, grant);
  }
  Async<void> release(std::string id, Context ctx) {
    std::shared_ptr<Entry> entry;
    {
      std::lock_guard lock(mutex);
      const auto found = entries.find(id);
      if (found == entries.end())
        throw Refusal("unknown_effect", "effect not in this activation");
      entry = found->second;
    }
    (co_await entry->release(ctx));
  }
  bool released() {
    std::lock_guard lock(mutex);
    for (auto &item : entries) {
      std::lock_guard entry_lock(item.second->mutex);
      if (!item.second->finished || item.second->failure)
        return false;
    }
    return true;
  }
};

class SequenceRegistry {
  struct Step {
    std::optional<Inverse> inverse;
    bool started = false, finished = false;
    std::exception_ptr failure;
    std::mutex mutex;
    corral::Event changed;
    Async<void> undo(Context ctx) {
      std::unique_lock lock(mutex);
      if (started) {
        lock.unlock();
        co_await changed;
        if (failure)
          std::rethrow_exception(failure);
        co_return;
      }
      if (!inverse)
        throw Refusal("unknown_inverse", "step has no inverse");
      started = true;
      lock.unlock();
      std::exception_ptr error;
      try {
        (co_await inverse->cleanup(ctx));
      } catch (...) {
        error = std::current_exception();
      }
      co_await ctx.finish();
      lock.lock();
      failure = error;
      if (!error)
        inverse.reset();
      finished = true;
      lock.unlock();
      changed.trigger();
      if (error)
        std::rethrow_exception(error);
    }
    bool released() {
      std::lock_guard lock(mutex);
      return !inverse && !failure;
    }
  };
  struct Entry {
    std::function<Async<EffectStep>(Context)> producer;
    std::mutex mutex;
    std::map<std::string, std::shared_ptr<Step>> steps;
    bool running = false, ended = false, released = false;
    std::uint64_t next = 1;
  };
  std::mutex mutex;
  std::map<std::string, std::shared_ptr<Entry>> entries;
  std::uint64_t next_id = 0;
  std::size_t total_steps = 0;
  std::shared_ptr<Entry> get(std::string id) {
    std::lock_guard lock(mutex);
    auto found = entries.find(id);
    if (found == entries.end())
      throw Refusal("unknown_sequence", id);
    return found->second;
  }

public:
  Async<EffectSequence>
  add(std::shared_ptr<DependencyChannel> channel, std::string context,
      std::string label, std::function<Async<EffectStep>(Context)> producer) {
    if (label.size() > 1024)
      throw Refusal("invalid_argument", "sequence label too long");
    auto entry = std::make_shared<Entry>();
    entry->producer = std::move(producer);
    std::string id;
    {
      std::lock_guard lock(mutex);
      if (next_id >= 256)
        throw Refusal("sequences_exhausted", "sequence history full");
      id = "s" + std::to_string(++next_id);
      entries.emplace(id, entry);
    }
    Json args = {{"context", context}, {"sequence", id}, {"label", label}};
    const auto reply =
        co_await channel->request("sequence_register", std::move(args));
    const auto grant = reply.at("grant").get<std::string>();
    if (reply.at("sequence") != id || grant.size() != 32 ||
        !std::all_of(grant.begin(), grant.end(), [](char c) {
          return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
        }))
      throw Refusal("protocol", "invalid sequence registration");
    co_return EffectSequence(channel, id, grant);
  }
  Async<Json> advance(std::string id, std::string number, Context ctx) {
    auto entry = get(id);
    {
      std::lock_guard lock(entry->mutex);
      if (entry->running || entry->ended || entry->released)
        throw Refusal("closed", "sequence cannot advance");
      if (number != std::to_string(entry->next))
        throw Refusal("invalid_step", "step order mismatch");
      {
        std::lock_guard registry_lock(mutex);
        if (total_steps >= 4096)
          throw Refusal("steps_exhausted", "step history full");
        ++total_steps;
      }
      ++entry->next;
      entry->running = true;
    }
    struct Guard {
      std::shared_ptr<Entry> entry;
      Context ctx;
      ~Guard() {
        std::lock_guard lock(entry->mutex);
        entry->running = false;
      }
    } guard{entry, ctx};
    EffectStep result = EffectStep::complete();
    try {
      result =
          (co_await native::with_cleanup([&] { return entry->producer(ctx); },
                                         [ctx] { return ctx.finish(); }));
    } catch (const RpcError &error) {
      result = EffectStep::failed(error);
    } catch (const std::exception &error) {
      result = EffectStep::failed(RpcError("step_failed", error.what()));
    } catch (...) {
      result =
          EffectStep::failed(RpcError("step_failed", "non-standard exception"));
    }
    auto step = std::make_shared<Step>();
    step->inverse = std::move(result.inverse);
    {
      std::lock_guard lock(entry->mutex);
      entry->steps.emplace(number, step);
      entry->ended = result.done;
    }
    if ((step->inverse && step->inverse->label.size() > 1024) ||
        (result.error && !result.done))
      throw Refusal("invalid_step", "invalid producer result");
    co_return Json{
        {"done", result.done},
        {"inverse", step->inverse ? Json(step->inverse->label) : Json(nullptr)},
        {"error", result.error ? Json{{"code", result.error->code},
                                      {"message", result.error->what()}}
                               : Json(nullptr)}};
  }
  Async<void> undo(std::string id, std::string number, Context ctx) {
    auto entry = get(id);
    std::shared_ptr<Step> step;
    {
      std::lock_guard lock(entry->mutex);
      auto found = entry->steps.find(number);
      if (found == entry->steps.end())
        throw Refusal("unknown_inverse", number);
      step = found->second;
    }
    (co_await step->undo(ctx));
  }
  void release(std::string id) {
    auto entry = get(id);
    std::lock_guard lock(entry->mutex);
    if (entry->running)
      throw Refusal("effects_remaining", "sequence step running");
    for (const auto &step : entry->steps)
      if (!step.second->released())
        throw Refusal("effects_remaining", "sequence inverses remain");
    entry->released = true;
    entry->ended = true;
    entry->producer = {};
  }
  bool released() {
    // Never take an entry lock under the registry lock: advance reserves its
    // global step budget while holding an entry's admission lock.
    std::vector<std::shared_ptr<Entry>> snapshot;
    {
      std::lock_guard lock(mutex);
      for (const auto &item : entries)
        snapshot.push_back(item.second);
    }
    for (auto &entry : snapshot) {
      std::lock_guard lock(entry->mutex);
      if (!entry->released)
        return false;
    }
    return true;
  }
};
inline Async<EffectSequence>
Context::effect(std::string label,
                std::function<Async<EffectStep>(Context)> producer) const {
  return cordis::detail::invoke_owned(
      [*this](std::string label,
              std::function<Async<EffectStep>(Context)> producer)
          -> Async<EffectSequence> {
        {
          std::lock_guard lock(frame->mutex);
          if (!frame->open || frame->token.empty() || frame->cleanup)
            throw Refusal("closed", "context cannot start sequences");
          if (!frame->sequences)
            throw Refusal("unsupported_sequences",
                          "sequence capability not negotiated");
          ++frame->calls;
        }
        Guard guard{frame};
        co_return (co_await frame->sequences->add(frame->channel, frame->token,
                                                  label, std::move(producer)));
      },
      std::move(label), std::move(producer));
}

inline Async<Effect>
Context::defer(std::string label,
               std::function<Async<void>(Context)> cleanup) const {
  return cordis::detail::invoke_owned(
      [*this](std::string label,
              std::function<Async<void>(Context)> cleanup) -> Async<Effect> {
        {
          std::lock_guard lock(frame->mutex);
          if (!frame->open || frame->token.empty() || frame->cleanup)
            throw Refusal("closed", "context cannot register effects");
          if (!frame->effects)
            throw Refusal("unsupported_effects",
                          "effects capability not negotiated");
          ++frame->calls;
        }
        Guard guard{frame};
        co_return (co_await frame->effects->add(frame->channel, frame->token,
                                                label, std::move(cleanup)));
      },
      std::move(label), std::move(cleanup));
}
} // namespace cordis::process

#include "events.hpp"
#include "scope.hpp"
#include "task.hpp"

#include "middleware.hpp"

#include "composition.hpp"
