#pragma once
#include "runtime.hpp"
#include <map>

namespace cordis::component::detail {
using CallbackFunction = std::function<Async<Value>(Context, Value)>;
using WireCallback = std::function<Async<Payload>(Context, Payload)>;
struct Callback {
  CallbackFunction run;
  WireCallback wire_run;
  bool once = false, running = false, finished = false;
  Payload result;
  std::exception_ptr error;
  Async<Payload> call(Context ctx, Payload args) {
    if (once && finished) {
      if (error)
        std::rethrow_exception(error);
      co_return result;
    }
    if (once && running)
      throw Refusal("busy", "callback already running");
    running = true;
    try {
      Payload value =
          wire_run ? co_await wire_run(std::move(ctx), std::move(args))
                   : Payload(co_await run(std::move(ctx), args.control_only()));
      running = false;
      if (once) {
        result = value;
        finished = true;
      }
      co_return value;
    } catch (...) {
      running = false;
      if (once) {
        error = std::current_exception();
        finished = true;
      }
      throw;
    }
  }
};
struct Callbacks {
  Validation validation = Validation::Checked;
  std::map<uint64_t, std::shared_ptr<Callback>> entries;
  uint64_t next = 0;
  uint64_t insert(CallbackFunction run, bool once = false) {
    if (entries.size() >= 4096 || next >= 65536)
      throw Refusal("overloaded", "callback capacity reached");
    auto callback = std::make_shared<Callback>();
    callback->run = std::move(run);
    callback->once = once;
    auto id = ++next;
    entries.emplace(id, std::move(callback));
    return id;
  }
  uint64_t insert(WireCallback run, bool once = false) {
    auto id = insert(CallbackFunction{}, once);
    entries.at(id)->wire_run = std::move(run);
    return id;
  }
  void remove(uint64_t id) { auto entry = entries.extract(id); }
  std::shared_ptr<Callback> get(uint64_t id) {
    auto found = entries.find(id);
    if (found == entries.end())
      throw Refusal("closed", "callback absent");
    return found->second;
  }
};
inline Async<Value> control(std::string command, std::string grant,
                            Value extra = Value::object()) {
  if (!current())
    throw Refusal("closed", "operation requires a live callback");
  extra["grant"] = std::move(grant);
  return request(current()->shared_from_this(), std::move(command),
                 std::move(extra));
}
inline bool valid_grant(const Value &value) {
  return value.is_string() &&
         value.get_ref<const std::string &>().size() == 32 &&
         std::all_of(value.get_ref<const std::string &>().begin(),
                     value.get_ref<const std::string &>().end(), [](char c) {
                       return (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f');
                     });
}
inline std::string grant(const Value &value) {
  if (!valid_grant(value))
    throw Unknown("invalid host grant");
  return value.get<std::string>();
}
struct ResourceCapability {
  std::string grant;
  std::weak_ptr<Callbacks> instance;
};
inline const ResourceCapability &
resource_capability(const std::any &state,
                    const std::shared_ptr<Callbacks> &instance) {
  const auto *resource = std::any_cast<ResourceCapability>(&state);
  if (!resource || resource->instance.lock() != instance)
    throw Refusal("invalid_handle",
                  "resource belongs to another instance or adapter");
  return *resource;
}
inline std::vector<Flow> decode_flows(const Payload &payload) {
  if (!payload.control.is_array())
    throw Unknown("missing event tags");
  std::size_t index = 0;
  std::vector<Flow> flows;
  for (const auto &tag : payload.control) {
    if (tag == "continue")
      flows.push_back(cordis::Continue());
    else if (tag == "break" && index < payload.messages.size())
      flows.push_back(cordis::Break(payload.messages[index++]));
    else
      throw Unknown("invalid event tag/body");
  }
  if (index != payload.messages.size())
    throw Unknown("unexpected event body");
  return flows;
}
inline Context context(std::shared_ptr<Frame> frame,
                       std::shared_ptr<Callbacks> callbacks) {
  auto req = [frame](std::string command, Value args) {
    return request(frame, std::move(command), std::move(args));
  };
  auto binary = [frame](std::string command, Payload args) {
    return request_payload(frame, std::move(command), std::move(args));
  };
  auto registration = [req,
                       callbacks](std::string command, Value args, auto run,
                                  bool once = false) -> Async<std::string> {
    auto id = callbacks->insert(std::move(run), once);
    args["callback"] = id;
    try {
      co_return grant(co_await req(std::move(command), std::move(args)));
    } catch (const RpcError &) {
      callbacks->remove(id);
      throw;
    }
  };
  Context::Operations ops;
  ops.validation = callbacks->validation;
  ops.acquire = [binary, callbacks](std::string dependency, std::string kind,
                                    Message args) -> Async<ResourceHandle> {
    Payload payload(Value{{"dependency", dependency}, {"kind", kind}}, args);
    auto id = grant(
        (co_await binary("dependency-resource-acquire", std::move(payload)))
            .control_only());
    co_return cordis::detail::ResourceAccess::make(
        ResourceCapability{id, callbacks});
  };
  ops.resource_invoke = [binary, callbacks](std::any state, std::string method,
                                            Message args) -> Async<Message> {
    const auto &resource = resource_capability(state, callbacks);
    Payload payload(Value{{"grant", resource.grant}, {"method", method}}, args);
    co_return (
        co_await binary("dependency-resource-invoke", std::move(payload)))
        .one();
  };
  ops.resource_dispose = [req, callbacks](std::any state) -> Async<void> {
    const auto &resource = resource_capability(state, callbacks);
    Value args = {{"grant", resource.grant}};
    if (!(co_await req("dependency-resource-dispose", std::move(args)))
             .is_null())
      throw Unknown("invalid resource disposal acknowledgement");
  };
  ops.invoke = [binary](std::string dependency, std::string method,
                        Message args) -> Async<Message> {
    Payload payload(Value{{"dependency", dependency}, {"method", method}},
                    args);
    co_return (co_await binary("invoke", std::move(payload))).one();
  };
  ops.metadata = [req](std::string name, std::string revision) {
    return req("metadata", {{"name", name}, {"revision", revision}});
  };
  ops.defer =
      [registration](
          std::string label,
          std::function<Async<void>(Context)> cleanup) -> Async<Effect> {
    Value args = {{"label", label}};
    auto id = co_await registration(
        "defer", std::move(args),
        [cleanup = std::move(cleanup)](Context ctx, Value) -> Async<Value> {
          co_await cleanup(std::move(ctx));
          co_return nullptr;
        },
        true);
    co_return Effect(
        [id]() -> Async<void> { co_await control("dispose", id); });
  };
  ops.effect = [registration,
                callbacks](std::string label,
                           std::function<Async<EffectStep>(Context)> next)
      -> Async<EffectSequence> {
    Value args = {{"label", label}};
    auto id = co_await registration(
        "effect", std::move(args),
        [next = std::move(next), callbacks](Context ctx,
                                            Value) -> Async<Value> {
          // Reserve cleanup storage before the producer can acquire anything.
          auto inverse_id = callbacks->insert(CallbackFunction{}, true);
          struct Reservation {
            Callbacks &callbacks;
            uint64_t id;
            ~Reservation() {
              if (id)
                callbacks.remove(id);
            }
          } reserved{*callbacks, inverse_id};
          auto slot = callbacks->get(inverse_id);
          auto step = co_await next(std::move(ctx));
          Value inverse = nullptr;
          if (step.inverse) {
            slot->run = [cleanup = std::move(step.inverse->cleanup)](
                            Context ctx, Value) -> Async<Value> {
              co_await cleanup(std::move(ctx));
              co_return nullptr;
            };
            inverse = inverse_id;
            reserved.id = 0;
          }
          co_return Value{{"inverse", inverse},
                          {"complete", step.done},
                          {"failure", step.error ? Value(step.error->what())
                                                 : Value(nullptr)}};
        });
    co_return EffectSequence(
        [id]() -> Async<std::string> {
          auto value =
              (co_await control("effect-ready", id)).get<std::string>();
          if (value == "Completed")
            co_return std::string("completed");
          if (value == "Stopped")
            co_return std::string("stopped");
          throw Unknown("invalid completion");
        },
        [id]() -> Async<void> { co_await control("effect-dispose", id); },
        [id] { return control("effect-status", id); });
  };
  ops.scope = [req, callbacks](std::string label) -> Async<Scope> {
    Value args = {{"label", label}};
    auto id = grant(co_await req("scope", std::move(args)));
    co_return Scope(
        [id]() -> Async<void> { co_await control("scope-ready", id); },
        [id]() -> Async<void> { co_await control("scope-dispose", id); },
        [id] { return control("scope-status", id); },
        [id,
         callbacks](std::function<Async<Value>(Context)> run) -> Async<Value> {
          auto callback = callbacks->insert(
              [run = std::move(run)](Context ctx, Value) {
                return run(std::move(ctx));
              },
              true);
          try {
            Value args = {{"callback", callback}};
            auto value = co_await control("scope-run", id, std::move(args));
            callbacks->remove(callback);
            co_return value;
          } catch (const RpcError &) {
            callbacks->remove(callback);
            throw;
          }
        });
  };
  ops.spawn =
      [registration](std::string label,
                     std::function<Async<void>(Context)> run) -> Async<Task> {
    Value args = {{"label", label}};
    auto id = co_await registration(
        "spawn", std::move(args),
        [run = std::move(run)](Context ctx, Value) -> Async<Value> {
          co_await run(std::move(ctx));
          co_return nullptr;
        },
        true);
    co_return Task([id]() -> Async<void> { co_await control("join", id); },
                   [id] { return control("task-status", id); });
  };
  ops.cancelled_for = [req](std::chrono::milliseconds duration) -> Async<bool> {
    co_return (co_await req("cancelled-for", duration.count())).get<bool>();
  };
  ops.cancelled = [req]() -> Async<void> {
    co_await req("cancelled", nullptr);
  };
  ops.on =
      [registration](
          std::string event,
          std::function<Async<Flow>(Context, Message)> run) -> Async<Effect> {
    Value args = {{"event", event}};
    auto id = co_await registration(
        "on", std::move(args),
        WireCallback([run = std::move(run)](Context ctx,
                                            Payload input) -> Async<Payload> {
          co_return session::flow_payload(
              co_await run(std::move(ctx), input.one()));
        }));
    co_return Effect(
        [id]() -> Async<void> { co_await control("dispose", id); });
  };
  ops.emit = [binary](std::string event, Message args) -> Async<Dispatch> {
    Payload payload(Value{{"event", event}}, args);
    auto id = grant(
        (co_await binary("event-emit", std::move(payload))).control_only());
    co_return Dispatch(
        [id]() -> Async<std::vector<Flow>> {
          if (!current())
            throw Refusal("closed", "operation requires live callback");
          Payload payload(Value{{"grant", id}});
          co_return decode_flows(co_await request_payload(
              current()->shared_from_this(), "event-join", std::move(payload)));
        },
        [id] { return control("event-status", id); });
  };
  ops.serial = [binary](std::string event, Message args) -> Async<Flow> {
    Payload payload(Value{{"event", event}, {"serial", true}}, args);
    auto flows = decode_flows(co_await binary("emit", std::move(payload)));
    for (auto &flow : flows)
      if (flow.value)
        co_return flow;
    co_return cordis::Continue();
  };
  ops.middleware =
      [registration](std::string name,
                     std::function<Async<Message>(Context, Message, Next)> run)
      -> Async<Effect> {
    Value args = {{"name", name}};
    auto id = co_await registration(
        "middleware", std::move(args),
        WireCallback([run = std::move(run)](Context ctx,
                                            Payload input) -> Async<Payload> {
          auto next = grant(input.control.at("next"));
          auto value = co_await run(
              std::move(ctx), input.one(),
              Next([next](Message value) -> Async<Message> {
                if (!current())
                  throw Refusal("closed", "operation requires live callback");
                Payload payload(Value{{"grant", next}}, value);
                co_return (
                    co_await request_payload(current()->shared_from_this(),
                                             "next", std::move(payload)))
                    .one();
              }));
          co_return Payload(value);
        }));
    co_return Effect(
        [id]() -> Async<void> { co_await control("dispose", id); });
  };
  ops.run = [binary, callbacks](
                std::string name, Message args,
                std::function<Async<Message>(Context, Message)> terminal)
      -> Async<Message> {
    auto id = callbacks->insert(
        WireCallback([terminal = std::move(terminal)](
                         Context ctx, Payload input) -> Async<Payload> {
          co_return Payload(co_await terminal(std::move(ctx), input.one()));
        }),
        true);
    try {
      Payload payload(Value{{"name", name}, {"callback", id}}, args);
      auto result = co_await binary("run", std::move(payload));
      callbacks->remove(id);
      co_return result.one();
    } catch (const RpcError &) {
      callbacks->remove(id);
      throw;
    }
  };
  ops.install = [binary](std::string factory, Message config) -> Async<Child> {
    Payload payload(Value{{"factory", factory}}, config);
    auto id =
        grant((co_await binary("install", std::move(payload))).control_only());
    co_return Child(
        [id] { return control("plugin-ready", id); },
        [id] { return control("plugin-settled", id); },
        [id] { return control("plugin-status", id); },
        [id]() -> Async<void> { co_await control("plugin-dispose", id); });
  };
  ops.provide = [binary](std::string service, Message value) -> Async<void> {
    Payload payload(Value{{"service", service}}, value);
    (co_await binary("provide", std::move(payload))).control_only();
  };
  return Context(std::move(ops));
}
} // namespace cordis::component::detail
