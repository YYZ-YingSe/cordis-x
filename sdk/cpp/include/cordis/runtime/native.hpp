#pragma once
#include <cordis/core/async.hpp>
#include <cordis/core/error.hpp>
#define ASIO_DISABLE_THREAD_KEYWORD_EXTENSION
#include <asio/detail/tss_ptr.hpp>
#include <corral/asio-standalone.h>
#include <mutex>
#include <thread>
#include <variant>

namespace cordis::native {
// Asio owns I/O, timers and cross-thread posting. Corral owns task execution
// and cancellation. An instance has one execution thread, never one per wait.
class Runtime {
  asio::io_context io_;
  asio::executor_work_guard<asio::io_context::executor_type> work_{
      io_.get_executor()};
  corral::UnsafeNursery tasks_{io_};
  std::mutex admission_;
  std::once_flag joined_;
  bool closing_ = false;
  inline static asio::detail::tss_ptr<Runtime> current_;
  std::thread worker_;

public:
  Runtime()
      : worker_([this] {
          current_ = this;
          io_.run();
          current_ = nullptr;
#if defined(__APPLE__)
          cordis::detail::executor_storage.retire();
#endif
        }) {
  }
  Runtime(const Runtime &) = delete;
  Runtime &operator=(const Runtime &) = delete;
  ~Runtime() { close(); }
  static Runtime &current() {
    if (!current_)
      throw Refusal("closed", "operation requires a native execution context");
    return *current_;
  }
  asio::io_context &io() { return io_; }
  template <class F> void post(F callback) {
    asio::post(io_, std::move(callback));
  }
  void submit(std::function<Async<void>()> run) {
    std::lock_guard lock(admission_);
    if (closing_)
      throw Refusal("closed", "native execution context is closing");
    post([this, run = std::move(run)]() mutable {
      tasks_.start(std::move(run));
    });
  }
  // Called only after transport admission has stopped. Outstanding callbacks
  // remain deliverable while Corral drains the owned tasks. ABI destruction
  // joins this thread before its containing library can be unloaded.
  void close() {
    std::call_once(joined_, [this] {
      {
        std::lock_guard lock(admission_);
        closing_ = true;
        post([this] { tasks_.asyncClose([this] { work_.reset(); }); });
      }
      if (worker_.get_id() == std::this_thread::get_id())
        std::terminate();
      worker_.join();
    });
  }
};

template <class T> using Outcome = std::variant<T, std::exception_ptr>;

// A transport must settle each admitted operation exactly once. Completion
// may arrive synchronously or from any thread; it is always posted back to
// the originating Corral task tree. Cancellation cannot discard a late result.
template <class T, class Submit> Async<T> completed(Submit submit) {
  auto &runtime = Runtime::current();
  auto result = co_await corral::noncancellable(
      corral::untilCBCalled([&](std::function<void(Outcome<T>)> resume) {
        submit([&runtime, resume = std::move(resume)](Outcome<T> result) {
          runtime.post([resume, result = std::move(result)]() mutable {
            resume(std::move(result));
          });
        });
      }));
  if (auto failure = std::get_if<std::exception_ptr>(&result))
    std::rethrow_exception(*failure);
  co_return std::move(std::get<T>(result));
}

// An observation may be withdrawn, but its foreign completion still owns
// callback storage. Cancellation asks the transport to stop and then keeps the
// coroutine alive until the one completion arrives. Ordinary completed() calls
// retain their stronger admitted-work semantics.
template <class T, class Submit, class Cancel> class ObservationCompletion {
  Runtime &runtime_ = Runtime::current();
  Submit submit_;
  Cancel cancel_;
  std::optional<Outcome<T>> result_;
public:
  ObservationCompletion(Submit submit, Cancel cancel)
      : submit_(std::move(submit)), cancel_(std::move(cancel)) {}
  bool await_ready() const noexcept { return false; }
  void await_suspend(std::coroutine_handle<> continuation) {
    submit_([this, continuation](Outcome<T> result) {
      runtime_.post([this, continuation, result = std::move(result)]() mutable {
        result_.emplace(std::move(result));
        continuation.resume();
      });
    });
  }
  std::true_type await_early_cancel() const noexcept { return {}; }
  bool await_cancel(std::coroutine_handle<>) noexcept {
    cancel_();
    return false;
  }
  bool await_must_resume() const noexcept { return false; }
  T await_resume() {
    if (auto failure = std::get_if<std::exception_ptr>(&*result_))
      std::rethrow_exception(*failure);
    return std::move(std::get<T>(*result_));
  }
};
template <class T, class Submit, class Cancel>
Async<T> observed(Submit submit, Cancel cancel) {
  co_return co_await ObservationCompletion<T, Submit, Cancel>(
      std::move(submit), std::move(cancel));
}
inline auto sleep(std::chrono::milliseconds duration) {
  return corral::sleepFor(Runtime::current().io(), duration);
}

// Asynchronous cleanup is explicit: a destructor cannot suspend to revoke
// admission and drain the operations already admitted by a callback.
template <class Body, class Cleanup>
auto with_cleanup(Body body, Cleanup cleanup) -> std::invoke_result_t<Body &> {
  using Result = typename std::invoke_result_t<Body &>::ReturnType;
  using Stored =
      std::conditional_t<std::is_void_v<Result>, std::monostate, Result>;
  std::optional<Stored> result;
  std::exception_ptr failure;
  co_await corral::try_([&]() -> Async<void> {
    try {
      if constexpr (std::is_void_v<Result>) {
        co_await body();
        result.emplace();
      } else {
        result.emplace(co_await body());
      }
    } catch (...) {
      failure = std::current_exception();
    }
  }).finally([&] { return cleanup(); });
  if (failure)
    std::rethrow_exception(failure);
  if constexpr (!std::is_void_v<Result>)
    co_return std::move(*result);
}
} // namespace cordis::native
