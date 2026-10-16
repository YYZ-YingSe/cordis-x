#pragma once
#include "context.hpp"

namespace cordis::process {
inline Json check_composition(Json manifest) {
  if (!manifest.is_object() || manifest.size() != 2 ||
      !manifest.contains("factories") || !manifest.at("factories").is_array() ||
      !manifest.contains("provisions") ||
      !manifest.at("provisions").is_object())
    throw Refusal("composition_mismatch", "invalid composition manifest");
  std::set<std::string> factories;
  for (const auto &name : manifest.at("factories")) {
    if (!name.is_string() || name.get<std::string>().empty() ||
        name.get<std::string>().size() > 1024 || !factories.insert(name).second)
      throw Refusal("composition_mismatch", "invalid factory alias");
  }
  for (const auto &item : manifest.at("provisions").items()) {
    const auto &contract = item.value();
    if (item.key().empty() || item.key().size() > 1024 ||
        !contract.is_object() || contract.size() != 2 ||
        !contract.contains("name") || !contract.at("name").is_string() ||
        contract.at("name").get<std::string>().empty() ||
        !contract.contains("revision") ||
        !contract.at("revision").is_string() ||
        contract.at("revision").get<std::string>().empty())
      throw Refusal("composition_mismatch", "invalid provision manifest");
  }
  manifest["factories"] = factories;
  return manifest;
}
class Child {
  std::shared_ptr<DependencyChannel> channel;
  Json identity;
  Async<Json> call(std::string command) const {
    return cordis::detail::invoke_owned(
        [*this](std::string command) -> Async<Json> {
          co_return (co_await channel->request(command, identity));
        },
        std::move(command));
  }

public:
  Child(std::shared_ptr<DependencyChannel> channel, Json reply)
      : channel(std::move(channel)), identity(std::move(reply)) {
    auto token = [](Json v) {
      if (!v.is_string())
        return false;
      const auto &s = v.get_ref<const std::string &>();
      return s.size() == 32 &&
             s.find_first_not_of("0123456789abcdef") == std::string::npos;
    };
    if (!identity.is_object() || identity.size() != 2 ||
        !identity.contains("child") || !token(identity.at("child")) ||
        !identity.contains("grant") || !token(identity.at("grant")))
      throw Refusal("protocol", "invalid child acknowledgement");
  }
  Async<Json> ready() const {
    return cordis::detail::invoke_owned(
        [*this]() -> Async<Json> { co_return co_await call("child_ready"); });
  }
  Async<Json> settled() const {
    return cordis::detail::invoke_owned(
        [*this]() -> Async<Json> { co_return co_await call("child_settled"); });
  }
  Async<Json> status() const {
    return cordis::detail::invoke_owned(
        [*this]() -> Async<Json> { co_return co_await call("child_status"); });
  }
  Async<void> dispose() const {
    return cordis::detail::invoke_owned(
        [*this]() -> Async<void> { co_await call("child_dispose"); });
  }
};
inline Async<Child> Context::install(std::string factory,
                                     Message config) const {
  return cordis::detail::invoke_owned(
      [*this](std::string factory, Message config) -> Async<Child> {
        {
          std::lock_guard lock(frame->mutex);
          if (!frame->open || frame->token.empty() || frame->cleanup)
            throw Refusal("closed", "context cannot install children");
          if (!frame->composition)
            throw Refusal("unsupported_composition",
                          "composition capability not negotiated");
          const auto &aliases = frame->composition->at("factories");
          if (std::find(aliases.begin(), aliases.end(), factory) ==
              aliases.end())
            throw Refusal("undeclared_factory", factory);
          ++frame->calls;
        }
        Guard guard{frame};
        auto reply = co_await frame->channel->request_payload(
            "child_install",
            Payload(Json{{"context", frame->token}, {"factory", factory}},
                    config));
        co_return Child(frame->channel,
                        operation_outcome(reply.control_only()));
      },
      std::move(factory), std::move(config));
}
inline Async<void> Context::provide(std::string service, Message value) const {
  return cordis::detail::invoke_owned(
      [*this](std::string service, Message value) -> Async<void> {
        {
          std::lock_guard lock(frame->mutex);
          if (!frame->open || frame->token.empty() || frame->cleanup)
            throw Refusal("closed", "context cannot publish values");
          if (!frame->composition)
            throw Refusal("unsupported_composition",
                          "composition capability not negotiated");
          if (!frame->composition->at("provisions").contains(service))
            throw Refusal("undeclared_provision", service);
          ++frame->calls;
        }
        Guard guard{frame};
        (co_await frame->channel->request_payload(
             "value_provide",
             Payload(Json{{"context", frame->token}, {"service", service}},
                     value)))
            .control_only();
      },
      std::move(service), std::move(value));
}
} // namespace cordis::process
