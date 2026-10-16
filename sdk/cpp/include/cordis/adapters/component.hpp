#pragma once
#include "component/context.hpp"
#include "component/resources.hpp"
#include "model.hpp"

namespace cordis::component {
using detail::Payload;
template <class Model> struct Plugin {
  inline static std::shared_ptr<Service> service;
  inline static std::shared_ptr<detail::Callbacks> callbacks =
      std::make_shared<detail::Callbacks>();
  inline static std::array<std::size_t, 2> calls{};
  inline static detail::Resources resources;
  inline static bool attempted = false, active = false, deactivated = false;
  static Async<Payload> dispatch(std::shared_ptr<detail::Frame> frame,
                                 std::string command, Payload payload) {
    const auto &args = payload.control;
    if (command == "create") {
      if (service)
        throw Refusal("closed", "instance already created");
      auto settings = payload.control_only();
      callbacks->validation = cordis::validation(settings.at("validation").get<std::string>());
      service = Model::create(settings.at("config"));
      if (!service)
        throw std::runtime_error("null service");
      co_return Payload(Value(nullptr));
    }
    if (!service)
      throw Refusal("closed", "instance absent");
    auto ctx = detail::context(frame, callbacks);
    if (command == "callback") {
      auto callback = callbacks->get(args.at("id").get<uint64_t>());
      auto input = payload;
      input.control = args.at("args");
      co_return co_await callback->call(ctx, std::move(input));
    }
    if (command == "forget") {
      callbacks->remove(args.get<uint64_t>());
      co_return Payload(Value(nullptr));
    }
    if (command == "activate") {
      if (attempted)
        throw Refusal("closed", "activation already attempted");
      attempted = true;
      co_await service->activate(ctx);
      active = true;
      co_return Payload(Value(nullptr));
    }
    if (command == "deactivate") {
      if (deactivated)
        co_return Payload(Value(nullptr));
      if (!resources.recovered())
        throw Refusal("owned", "resources remain unrecovered");
      active = false;
      co_await service->deactivate(ctx);
      deactivated = true;
      co_return Payload(Value(nullptr));
    }
    if (command == "close") {
      if (!resources.recovered())
        throw Refusal("owned", "resources remain unrecovered");
      if (attempted && !deactivated)
        throw Refusal("owned", "instance not deactivated");
      auto removed = std::move(service);
      auto entries = std::move(callbacks->entries);
      co_return Payload(Value(nullptr));
    }
    if (command == "invoke") {
      if (!active)
        throw Refusal("closed", "instance inactive");
      co_return Payload(co_await service->invoke(
          ctx, args.at("method").get<std::string>(), payload.one()));
    }
    if (command == "resource-acquire" || command == "resource-invoke" ||
        command == "resource-release") {
      if (!active)
        throw Refusal("closed", "instance inactive");
      if (command == "resource-acquire")
        co_return co_await resources.acquire(*service, ctx, payload);
      if (command == "resource-invoke")
        co_return Payload(co_await resources.invoke(ctx, payload));
      co_await resources.release(ctx, args);
      co_return Payload(Value(nullptr));
    }
    throw Refusal("unknown_command", command);
  }
  static void describe(exports_cordis_plugin_managed_service_manifest_t *out) {
    auto deps = Model::dependencies(),
         interfaces = Model::dependency_interfaces();
    if (!deps.is_object() || !interfaces.is_object() ||
        deps.size() != interfaces.size())
      throw std::runtime_error(
          "dependency descriptor keys must match dependencies");
    auto provided = contract::normalize(Model::interface());
    for (auto it = deps.begin(); it != deps.end(); ++it)
      interfaces.at(it.key()) = contract::normalize(interfaces.at(it.key()));
    *out = {};
    out->provides = {detail::owned(Model::contract_name()),
                     detail::owned(Model::contract_revision()),
                     detail::Buffer(provided).release()};
    out->requires_ = {
        detail::allocate<cordis_plugin_types_contract_t>(deps.size()),
        deps.size()};
    std::size_t i = 0;
    for (auto it = deps.begin(); it != deps.end(); ++it)
      out->requires_.ptr[i++] = {
          detail::owned(it.key()),
          detail::owned(it.value().template get<std::string>()),
          detail::Buffer(interfaces.at(it.key())).release()};
  }
  static managed_plugin_callback_code_t call(managed_plugin_string_t *token,
                                             bool cleanup,
                                             managed_plugin_string_t *command,
                                             detail::RawPayload *args) {
    // Async exports own lowered argument storage; copy before scheduling and
    // release the canonical allocations even if decoding refuses the call.
    struct Inputs {
      managed_plugin_string_t *token, *command;
      detail::RawPayload *args;
      ~Inputs() {
        managed_plugin_string_free(token);
        managed_plugin_string_free(command);
        cordis_plugin_types_payload_free(args);
      }
    } inputs{token, command, args};
    auto holder =
        new std::shared_ptr<detail::Frame>(std::make_shared<detail::Frame>());
    auto frame = *holder;
    managed_plugin_context_set_0(holder);
    frame->cleanup = cleanup;
    try {
      if (calls[cleanup] >= 128)
        throw Refusal("overloaded", "call capacity reached");
      frame->token = detail::text(*token);
      auto method = detail::text(*command);
      frame->validation = service ? callbacks->validation : Validation::Audit;
      auto value = detail::decode(*args, frame->validation);
      ++calls[cleanup];
      frame->tasks.start(detail::execute, frame,
                         std::function<Async<Payload>()>(
                             [frame, method = std::move(method),
                              value = std::move(value)]() -> Async<Payload> {
                               struct Admission {
                                 bool cleanup;
                                 ~Admission() { --calls[cleanup]; }
                               } admission{frame->cleanup};
                               co_return co_await dispatch(frame, method,
                                                           value);
                             }));
    } catch (...) {
      frame->error = std::current_exception();
      frame->open = false;
      frame->done = true;
    }
    return detail::finish(holder);
  }
  static managed_plugin_callback_code_t
  callback(managed_plugin_event_t *event) {
    auto holder = static_cast<std::shared_ptr<detail::Frame> *>(
        managed_plugin_context_get_0());
    (*holder)->complete(*event);
    return detail::finish(holder);
  }
};
} // namespace cordis::component
#define CORDIS_COMPONENT_PLUGIN(Model)                                         \
  extern "C" void exports_cordis_plugin_managed_service_describe(              \
      exports_cordis_plugin_managed_service_manifest_t *out) {                 \
    ::cordis::component::Plugin<Model>::describe(out);                         \
  }                                                                            \
  extern "C" managed_plugin_callback_code_t                                    \
  exports_cordis_plugin_managed_service_call(                                  \
      managed_plugin_string_t *ctx, bool cleanup,                              \
      managed_plugin_string_t *cmd,                                            \
      exports_cordis_plugin_managed_service_payload_t *args) {                 \
    return ::cordis::component::Plugin<Model>::call(ctx, cleanup, cmd, args);  \
  }                                                                            \
  extern "C" managed_plugin_callback_code_t                                    \
  exports_cordis_plugin_managed_service_call_callback(                         \
      managed_plugin_event_t *event) {                                         \
    return ::cordis::component::Plugin<Model>::callback(event);                \
  }
