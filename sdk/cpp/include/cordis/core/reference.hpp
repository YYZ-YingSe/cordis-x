#pragma once
#include "service.hpp"

namespace cordis {
// A reference names one object for one host lifetime. It contains no local
// pointer, process address or service selector. The current Context chooses the
// physical adapter; therefore the same value can cross execution backends.
class InterfaceRef {
  std::string key_, digest_;

public:
  InterfaceRef(std::string key, std::string digest)
      : key_(std::move(key)), digest_(std::move(digest)) {}
  const std::string &key() const { return key_; }
  static Async<InterfaceRef> export_object(Context ctx, Value descriptor,
                                           std::string digest,
                                           std::shared_ptr<Resource> object) {
    co_return InterfaceRef(
        co_await ctx.export_reference(std::move(descriptor), std::move(object)),
        std::move(digest));
  }
  Async<Message> invoke(Context ctx, std::string method, Message args) const {
    return ctx.invoke_reference(key_, digest_, std::move(method),
                                std::move(args));
  }
  Async<void> dispose(Context ctx) const { return ctx.dispose_reference(key_); }
};
} // namespace cordis
