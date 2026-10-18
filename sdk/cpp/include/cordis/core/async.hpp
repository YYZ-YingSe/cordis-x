#pragma once

#include <algorithm>
#include <coroutine>
#include <cstdlib>
#include <exception>
#include <functional>
#include <utility>

#if defined(__wasm__)
// Each component instance executes on one thread. Corral documents this
// customization for targets without thread-local storage.
#define CORRAL_THREAD_LOCAL static
#endif
#include "executor_storage.hpp"
#include <corral/corral.h>

namespace cordis {
template <class T = void> using Async = corral::Task<T>;

// Construct initializer-list request values in a statement before co_await.
// Their backing arrays then die before suspension; GCC 13 cannot lower the
// equivalent nested construction inside an await expression.

namespace detail {
// A ready failure has a result type, so even a coroutine that can only reject
// can co_return its await expression without a dead return or missing value.
template <class T, class Error> struct Failed {
  Error error;
  bool await_ready() const noexcept { return true; }
  void await_suspend(std::coroutine_handle<>) const noexcept {}
  T await_resume() { throw std::move(error); }
};
template <class T, class Error> auto failed(Error error) {
  return Failed<T, Error>{std::move(error)};
}

// Coroutine lambdas borrow their closure. Keep the operation closure and its
// arguments in this coroutine frame until its child has actually completed.
template <class F, class... Args>
auto invoke_owned(F function, Args... args)
    -> std::invoke_result_t<F &, Args...> {
  if constexpr (std::is_void_v<
                    typename std::invoke_result_t<F &, Args...>::ReturnType>) {
    co_await std::invoke(function, std::move(args)...);
  } else {
    co_return co_await std::invoke(function, std::move(args)...);
  }
}
} // namespace detail
} // namespace cordis
