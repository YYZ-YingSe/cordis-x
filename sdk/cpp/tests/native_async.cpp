#include <cassert>
#include <cordis/runtime/native.hpp>
#include <latch>

int main() {
  std::latch finished(1);
  std::thread completion;
  std::exception_ptr failure;
  cordis::native::Runtime runtime;
  runtime.submit([&]() -> cordis::Async<void> {
    try {
      const auto owner = std::this_thread::get_id();
      auto immediate =
          co_await cordis::native::completed<int>([](auto done) { done(7); });
      assert(immediate == 7 && std::this_thread::get_id() == owner);
      auto remote = co_await cordis::native::completed<int>(
          [&](auto done) { completion = std::thread([done] { done(42); }); });
      assert(remote == 42 && std::this_thread::get_id() == owner);
      co_await cordis::native::sleep(std::chrono::milliseconds(1));
      assert(std::this_thread::get_id() == owner);
      bool cleaned = false;
      try {
        co_await cordis::native::with_cleanup(
            []() -> cordis::Async<void> {
              throw std::runtime_error("body");
              co_return;
            },
            [&]() -> cordis::Async<void> {
              co_await cordis::native::sleep(std::chrono::milliseconds(1));
              cleaned = true;
            });
        assert(false);
      } catch (const std::runtime_error &) {
        assert(cleaned);
      }
      // Cancellation must finish asynchronous cleanup before the losing task
      // is destroyed, even when its business body never returns.
      bool entered = false, cancelled_cleanup = false;
      co_await corral::anyOf(
          cordis::native::with_cleanup(
              [&]() -> cordis::Async<void> {
                entered = true;
                co_await corral::suspendForever;
              },
              [&]() -> cordis::Async<void> {
                co_await cordis::native::sleep(std::chrono::milliseconds(1));
                cancelled_cleanup = true;
              }),
          cordis::native::sleep(std::chrono::milliseconds(1)));
      assert(entered && cancelled_cleanup);
    } catch (...) {
      failure = std::current_exception();
    }
    finished.count_down();
  });
  finished.wait();
  if (completion.joinable())
    completion.join();
  runtime.close();
  if (failure)
    std::rethrow_exception(failure);
}
