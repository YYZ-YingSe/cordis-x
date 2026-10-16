#pragma once
#include <cordis/adapters/abi/context.hpp>
namespace cordis::abi {
// Copyable token; destruction does not retire the child. Parent ownership is
// held by Runtime, while this token may be used after its Context grant
// expires.
class Child {
  uint64_t host_ = 0, id_ = 0;
  const CordisAbiHostApi *base_ = nullptr;
  const CordisAbiCompositionHostApi *api_ = nullptr;
  friend class Context;
  Child(uint64_t host, uint64_t id, const CordisAbiHostApi *base,
        const CordisAbiCompositionHostApi *api)
      : host_(host), id_(id), base_(base), api_(api) {}
  Async<Value> call(uint32_t operation) const {
    return cordis::detail::invoke_owned(
        [*this](uint32_t operation) -> Async<Value> {
          if (!api_)
            throw Refusal("closed", "empty child");
          detail::Output input;
          detail::Budget budget;
          auto value = input.build(Value(nullptr), budget);
          co_return (co_await detail::receive(
                         base_, host_, false,
                         [&](uint64_t caller, CordisAbiCompletion completion) {
                           api_->call(host_, 0, operation, id_, {}, &value,
                                      caller, completion);
                         }))
              .first.control_only();
        },
        std::move(operation));
  }

public:
  Child() = default;
  Async<Value> ready() const {
    return cordis::detail::invoke_owned([*this]() -> Async<Value> {
      co_return (co_await call(CORDIS_ABI_CHILD_READY));
    });
  }
  Async<Value> settled() const {
    return cordis::detail::invoke_owned([*this]() -> Async<Value> {
      co_return (co_await call(CORDIS_ABI_CHILD_SETTLED));
    });
  }
  Async<Value> status() const {
    return cordis::detail::invoke_owned([*this]() -> Async<Value> {
      co_return (co_await call(CORDIS_ABI_CHILD_STATUS));
    });
  }
  Async<void> dispose() const {
    return cordis::detail::invoke_owned([*this]() -> Async<void> {
      (co_await call(CORDIS_ABI_CHILD_DISPOSE));
      co_return;
    });
  }
};
inline Async<Child> Context::install(std::string factory,
                                     Message config) const {
  return cordis::detail::invoke_owned(
      [*this](std::string factory, Message config) -> Async<Child> {
        if (!composition_)
          throw Refusal("unsupported", "Composition extension unavailable");
        detail::Output input;
        detail::Budget budget;
        auto raw = input.build(config, budget);
        auto result = (co_await detail::receive(
            raw_.api, raw_.host, true,
            [&](uint64_t caller, CordisAbiCompletion completion) {
              composition_->call(
                  raw_.host, raw_.grant, CORDIS_ABI_CHILD_INSTALL, 0,
                  detail::slice(factory), &raw, caller, completion);
            }));
        co_return Child(raw_.host, result.second, raw_.api, composition_);
      },
      std::move(factory), std::move(config));
}
inline Async<void> Context::provide(std::string alias, Message value) const {
  return cordis::detail::invoke_owned(
      [*this](std::string alias, Message value) -> Async<void> {
        if (!composition_)
          throw Refusal("unsupported", "Composition extension unavailable");
        detail::Output input;
        detail::Budget budget;
        auto raw = input.build(value, budget);
        (co_await detail::receive(
            raw_.api, raw_.host, false,
            [&](uint64_t caller, CordisAbiCompletion completion) {
              composition_->call(raw_.host, raw_.grant, CORDIS_ABI_PROVIDE, 0,
                                 detail::slice(alias), &raw, caller,
                                 completion);
            }));

        co_return;
      },
      std::move(alias), std::move(value));
}
} // namespace cordis::abi
