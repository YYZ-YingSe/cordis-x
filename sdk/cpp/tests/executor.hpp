#pragma once
#include <cordis/runtime/native.hpp>
#include <future>

// Blocking belongs only to the test controller. Every SDK operation and
// callback runs on the same native executor used by production adapters.
struct TestExecutor {
  cordis::native::Runtime runtime;
  template <class F> auto start(F function) {
    using T = typename std::invoke_result_t<F &>::ReturnType;
    auto result = std::make_shared<std::promise<T>>();
    auto future = result->get_future();
    runtime.submit([function = std::move(function),
                    result]() mutable -> cordis::Async<void> {
      try {
        if constexpr (std::is_void_v<T>) {
          co_await function();
          result->set_value();
        } else {
          result->set_value(co_await function());
        }
      } catch (...) {
        result->set_exception(std::current_exception());
      }
    });
    return future;
  }
  template <class T> T wait(cordis::Async<T> task) {
    auto owned = std::make_shared<cordis::Async<T>>(std::move(task));
    return start([owned]() mutable { return std::move(*owned); }).get();
  }
};
