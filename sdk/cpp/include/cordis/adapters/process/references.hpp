#pragma once
#include "context.hpp"
namespace cordis::process {
inline Async<std::string>
Context::reference_export(Value descriptor,
                          ReferenceRegistry::Callback callback) const {
  return cordis::detail::invoke_owned(
      [*this](Value descriptor,
              ReferenceRegistry::Callback callback) -> Async<std::string> {
        {
          std::lock_guard lock(frame->mutex);
          if (!frame->open || frame->cleanup || frame->token.empty() ||
              !frame->references)
            throw Refusal("closed", "export requires a live ordinary Context");
          ++frame->calls;
        }
        Guard guard{frame};
        const auto id = frame->references->insert(std::move(callback));
        try {
          Json args = {{"context", frame->token},
                       {"callback", id},
                       {"interface", std::move(descriptor)}};
          co_return (co_await frame->channel->request("reference_export",
                                                      std::move(args)))
              .get<std::string>();
        } catch (const RpcError &) {
          frame->references->remove(id);
          throw;
        }
      },
      std::move(descriptor), std::move(callback));
}
inline Async<Message> Context::reference_invoke(std::string key,
                                                std::string digest,
                                                std::string method,
                                                Message args) const {
  return cordis::detail::invoke_owned(
      [*this](std::string key, std::string digest, std::string method,
              Message args) -> Async<Message> {
        {
          std::lock_guard lock(frame->mutex);
          if (!frame->open || frame->token.empty())
            throw Refusal("closed", "reference requires a live Context");
          ++frame->calls;
        }
        Guard guard{frame};
        Payload payload(Json{{"context", frame->token},
                             {"reference", key},
                             {"digest", digest},
                             {"method", method}},
                        args);
        co_return (co_await frame->channel->request_payload("reference_invoke",
                                                            std::move(payload)))
            .one();
      },
      std::move(key), std::move(digest), std::move(method), std::move(args));
}
inline Async<void> Context::reference_dispose(std::string key) const {
  return cordis::detail::invoke_owned(
      [*this](std::string key) -> Async<void> {
        {
          std::lock_guard lock(frame->mutex);
          if (!frame->open || frame->token.empty())
            throw Refusal("closed", "reference requires a live Context");
          ++frame->calls;
        }
        Guard guard{frame};
        Json args = {{"context", frame->token}, {"reference", key}};
        co_await frame->channel->request("reference_dispose", std::move(args));
      },
      std::move(key));
}
} // namespace cordis::process
