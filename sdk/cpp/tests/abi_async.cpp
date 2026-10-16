#include "executor.hpp"
#include <cassert>
#include <cordis/adapters/abi/abi.hpp>

int main() {
  TestExecutor first, second;
  std::vector<std::future<void>> calls;
  for (uint64_t id = 1; id <= 16; ++id) {
    auto &executor = id % 2 ? first : second;
    calls.push_back(executor.start([id]() -> cordis::Async<void> {
      for (unsigned n = 0; n < 128; ++n) {
        co_await cordis::abi::detail::with_caller(
            {7, id, id % 2 ? cordis::Validation::Audit : cordis::Validation::Contract}, [id]() -> cordis::Async<void> {
              assert((co_await cordis::abi::detail::current_caller(7)).grant == id);
              assert((co_await cordis::abi::detail::current_caller(7)).validation ==
                     (id % 2 ? cordis::Validation::Audit : cordis::Validation::Contract));
              auto result = co_await cordis::native::completed<unsigned>(
                  [](auto done) { done(42u); });
              assert(result == 42);
              assert((co_await cordis::abi::detail::current_caller(7)).grant == id);
              assert((co_await cordis::abi::detail::current_caller(7)).validation ==
                     (id % 2 ? cordis::Validation::Audit : cordis::Validation::Contract));
              // This is the final operation in this invocation. Its callback
              // portal still uses the executor after it resumes the
              // invocation's parent.
            });
      }
      for (bool suspend : {false, true}) {
        try {
          co_await cordis::abi::detail::with_caller(
              {7, id}, [suspend]() -> cordis::Async<void> {
                if (suspend)
                  co_await cordis::native::completed<bool>(
                      [](auto done) { done(true); });
                throw cordis::RpcError("expected", "callback failure");
              });
          assert(false);
        } catch (const cordis::RpcError &error) {
          assert(error.code == "expected");
        }
      }
    }));
  }
  for (auto &call : calls)
    call.get();
}
