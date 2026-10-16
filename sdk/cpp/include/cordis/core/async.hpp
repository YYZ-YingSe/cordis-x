#pragma once

#include <algorithm>
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

namespace detail {
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
