#pragma once
#include "context.hpp"
#include <cordis/core/typed.hpp>
#include <cordis/protocols/session/binary.hpp>

namespace cordis::adapters {
// One object dispatcher is shared by all C++ execution adapters. Only the
// adapter's callback storage and host command differ; business code receives
// the same Context and generated method signature everywhere.
inline Async<session::Payload> reference_call(std::shared_ptr<Resource> object,
                                     Value descriptor, Context ctx,
                                     session::Payload args) {
  if (args.control.value("close", false)) {
    co_await object->close(std::move(ctx));
    co_return session::Payload(nullptr);
  }
  const auto method = args.control.at("method").get<std::string>();
  const auto signature = contract::operation(descriptor.at("methods"), method);
  auto input = args.one();
  if (!object->typed_dispatch())
    contract::check_value(signature.at("args"), input, "args",
                          ctx.validation());
  auto output = co_await typed::checked(signature.at("errors"), [&] {
    return object->invoke(ctx, method, std::move(input));
  });
  if (!object->typed_dispatch())
    contract::check_value(signature.at("result"), output, "result",
                          ctx.validation());
  co_return session::Payload(nullptr, std::move(output));
}
template <class Backend>
void references(Context::Operations &ops, const Backend &backend) {
  ops.reference_export =
      [backend](Value descriptor,
                std::shared_ptr<Resource> object) -> Async<std::string> {
    // A known refusal leaves ownership with the exporter. Unknown outcomes stay
    // retained by the adapter callback slot until the host recovers its scope.
    co_return co_await backend.reference_export(
        descriptor, [object, descriptor](auto ctx, session::Payload args) {
          return reference_call(object, descriptor, context(std::move(ctx)),
                                std::move(args));
        });
  };
  ops.reference_invoke = [backend](std::string key, std::string digest,
                                   std::string method, Message args) {
    return backend.reference_invoke(std::move(key), std::move(digest),
                                    std::move(method), std::move(args));
  };
  ops.reference_dispose = [backend](std::string key) {
    return backend.reference_dispose(std::move(key));
  };
}
} // namespace cordis::adapters
