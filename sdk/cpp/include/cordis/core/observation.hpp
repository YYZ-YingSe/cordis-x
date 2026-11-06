#pragma once
#include "async.hpp"
#include "error.hpp"
#include <exception>
#include <memory>
#include <mutex>
#include <optional>
#include <variant>

namespace cordis::detail {
// Only terminal observations live with the public handle. Failed admissions
// can retry; the adapter continues to own single-execution and failure barriers.
template <class T> class Observation {
  using Stored = std::conditional_t<std::is_void_v<T>, std::monostate, T>;
  struct State { std::mutex mutex; std::optional<Stored> value; std::exception_ptr failure; };
  std::shared_ptr<State> state = std::make_shared<State>();
public:
  template <class F> Async<T> observe(F function) const {
    return invoke_owned([state = state, function = std::move(function)]() mutable -> Async<T> {
      {
        std::lock_guard lock(state->mutex);
        if (state->failure) std::rethrow_exception(state->failure);
        if (state->value) {
          if constexpr (std::is_void_v<T>) co_return;
          else co_return *state->value;
        }
      }
      try {
        if constexpr (std::is_void_v<T>) {
          co_await function();
          std::lock_guard lock(state->mutex);
          state->value.emplace();
        } else {
          auto value = co_await function();
          { std::lock_guard lock(state->mutex); state->value = value; }
          co_return value;
        }
      } catch (const Refusal &) {
        throw;
      } catch (...) {
        std::lock_guard lock(state->mutex);
        state->failure = std::current_exception();
        throw;
      }
    });
  }
};
template <class T> std::function<Async<T>()> observed(std::function<Async<T>()> function) {
  return [cache = Observation<T>{}, function = std::move(function)] { return cache.observe(function); };
}
} // namespace cordis::detail
