#include "binary.hpp"
#include "executor.hpp"
#include <cassert>
#include <cordis/adapters/abi.hpp>
#include <cordis/adapters/process.hpp>

using cordis::Value;
static uint64_t live_grant = 1;
static TestExecutor &executor() {
  static TestExecutor executor;
  return executor;
}
template <class T> cordis::Async<T> scoped(cordis::Async<T> task) {
  using Stored = std::conditional_t<std::is_void_v<T>, std::monostate, T>;
  std::optional<Stored> result;
  co_await cordis::abi::detail::with_caller(
      {7, live_grant}, [&]() -> cordis::Async<void> {
        if constexpr (std::is_void_v<T>) {
          co_await std::move(task);
          result.emplace();
        } else
          result.emplace(co_await std::move(task));
      });
  if constexpr (!std::is_void_v<T>)
    co_return std::move(*result);
}
template <class T> T run_task(cordis::Async<T> task) {
  return executor().wait(scoped(std::move(task)));
}

template <class F> void rejected(const std::string &code, F call) {
  try {
    run_task(call());
    throw std::runtime_error("expected resource rejection");
  } catch (const cordis::RpcError &error) {
    assert(error.code == code);
  }
}
struct Channel : test::Peer {
  template <class Call, class Reply> auto run(Call call, Reply reply) {
    auto result =
        executor().start([call = std::move(call)] { return scoped(call()); });
    auto request = next();
    auto value = reply(Value{{"command", request.command},
                             {"payload", request.payload.control}});
    using namespace cordis::session;
    Outcome outcome;
    if (value.at("status") == "unknown")
      outcome = {Outcome::Kind::unknown, Payload{}, "lost", "lost"};
    else
      outcome = {Outcome::Kind::success,
                 request.command == "dependency_resource_call" ||
                         request.command == "cleanup_resource_call"
                     ? Payload(Value{{"status", "ok"}, {"value", nullptr}},
                               test::body(value.at("value")))
                     : Payload(value),
                 {},
                 {}};
    channel->receive(response(request, std::move(outcome)));
    return result.get();
  }
};
static void process_contexts() {
  Channel wire;
  cordis::process::Context first(wire.channel, "first", {{"counters", "1"}});
  const auto before = cordis::adapters::context(first);
  rejected("undeclared_dependency", [&] {
    return before.acquire("missing", "counter", test::body(nullptr));
  });
  auto handle = wire.run(
      [&] {
        return before.acquire("counters", "counter", test::body(nullptr));
      },
      [](const Value &request) {
        return Value{{"status", "ok"},
                     {"value",
                      {{"resource", request["payload"]["resource"]},
                       {"grant", std::string(32, 'b')}}}};
      });
  run_task(first.finish());
  rejected("closed",
           [&] { return handle.invoke(before, "get", test::body(nullptr)); });
  rejected("closed", [&] { return handle.dispose(before); });
  cordis::process::Context next(wire.channel, "next", {{"counters", "1"}});
  auto current = cordis::adapters::context(next);
  assert(test::json(wire.run(
             [&] { return handle.invoke(current, "get", test::body(nullptr)); },
             [](const Value &request) {
               assert(request["payload"]["context"] == "next");
               return Value{{"status", "ok"}, {"value", 42}};
             })) == 42);
  Channel foreign;
  auto other = cordis::adapters::context(
      cordis::process::Context(foreign.channel, "other", {{"counters", "1"}}));
  rejected("invalid_handle",
           [&] { return handle.invoke(other, "get", test::body(nullptr)); });
  rejected("invalid_handle", [&] { return handle.dispose(other); });
  auto cleanup = cordis::adapters::context(cordis::process::Context(
      wire.channel, "cleanup", {{"counters", "1"}}, {}, true));
  rejected("closed", [&] {
    return cleanup.acquire("counters", "counter", test::body(nullptr));
  });
  rejected("closed", [&] { return handle.dispose(cleanup); });
  wire.run([&] { return handle.invoke(cleanup, "get", test::body(nullptr)); },
           [](const Value &request) {
             assert(request["command"] == "cleanup_resource_call");
             return Value{{"status", "ok"}, {"value", 42}};
           });
  wire.run([&] { return handle.dispose(current); },
           [](const Value &) {
             return Value{{"status", "ok"}, {"value", nullptr}};
           });
  try {
    wire.run(
        [&] {
          return current.acquire("counters", "counter", test::body(nullptr));
        },
        [](const Value &) {
          return Value{{"status", "unknown"},
                       {"error", {{"code", "lost"}, {"message", "lost"}}}};
        });
    throw std::logic_error("expected unknown acquisition");
  } catch (const cordis::RpcError &) {
    throw std::logic_error("uncertain acquisition became a known rejection");
  } catch (const std::runtime_error &) {
  }
  run_task(next.finish());
  wire.channel->close();
  foreign.channel->close();
}

namespace abi = cordis::abi;
static int released_results = 0;
static void resource_call(uint64_t, uint64_t grant, uint32_t op,
                          uint64_t target, CordisAbiSlice, CordisAbiSlice,
                          const CordisAbiBuffer *, uint64_t caller,
                          CordisAbiCompletion done) {
  assert(caller == live_grant);
  auto result = abi::detail::protect([&] {
    if (grant != live_grant)
      throw cordis::Refusal("closed", "expired Context");
    if (op == CORDIS_ABI_DEPENDENCY_ACQUIRE)
      return abi::detail::result(nullptr, 42);
    assert(target == 42);
    if (op == CORDIS_ABI_DEPENDENCY_RESOURCE_CALL)
      return abi::detail::result(test::body(42));
    return abi::detail::result();
  });
  abi::detail::complete(done, result);
}
static void abi_contexts() {
  CordisAbiHostApi base{};
  base.version = CORDIS_ABI_CONTEXT_VERSION;
  base.struct_size = sizeof(base);
  base.invoke = [](uint64_t, uint64_t, CordisAbiSlice, CordisAbiSlice,
                   const CordisAbiBuffer *, uint64_t,
                   CordisAbiCompletion done) {
    abi::detail::complete(done, abi::detail::result());
  };
  base.defer = [](uint64_t, uint64_t, uint64_t, CordisAbiSlice, uint64_t,
                  CordisAbiCompletion done) {
    abi::detail::complete(done, abi::detail::result());
  };
  base.dispose = [](uint64_t, uint64_t, uint64_t, CordisAbiCompletion done) {
    abi::detail::complete(done, abi::detail::result());
  };
  base.release_result = [](CordisAbiResult *result) {
    ++released_results;
    abi::detail::release(result);
  };
  CordisAbiResourceHostApi resources{CORDIS_ABI_RESOURCES_VERSION,
                                     sizeof(CordisAbiResourceHostApi),
                                     resource_call};
  auto registry = std::make_shared<abi::detail::Callbacks>();
  auto context = [&](uint64_t grant, auto owner) {
    return cordis::adapters::context(abi::Context(
        {7, grant, &base}, owner, nullptr, nullptr, nullptr, &resources));
  };
  const auto first = context(1, registry);
  auto resource =
      run_task(first.acquire("counters", "counter", test::body(nullptr)));
  live_grant = 2;
  const auto current = context(2, registry);
  rejected("closed",
           [&] { return resource.invoke(first, "get", test::body(nullptr)); });
  rejected("closed", [&] { return resource.dispose(first); });
  assert(test::json(run_task(
             resource.invoke(current, "get", test::body(nullptr)))) == 42);
  auto other = context(2, std::make_shared<abi::detail::Callbacks>());
  rejected("invalid_handle",
           [&] { return resource.invoke(other, "get", test::body(nullptr)); });
  rejected("invalid_handle", [&] { return resource.dispose(other); });
  run_task(resource.dispose(current));
  assert(released_results == 5);
  Channel wire;
  auto process = cordis::adapters::context(
      cordis::process::Context(wire.channel, "current", {{"counters", "1"}}));
  rejected("invalid_handle", [&] {
    return resource.invoke(process, "get", test::body(nullptr));
  });
  wire.channel->close();
}
void failure_categories() {
  auto business = abi::detail::protect([]() -> CordisAbiResult {
    throw cordis::RpcError("closed", "business");
  });
  auto refusal = abi::detail::protect(
      []() -> CordisAbiResult { throw cordis::Refusal("closed", "runtime"); });
  assert(business.status == CORDIS_ABI_REJECTED);
  assert(refusal.status == CORDIS_ABI_REFUSED);
  abi::detail::release(&business);
  abi::detail::release(&refusal);
}
static void carrier_results() {
  CordisAbiHostApi api{};
  api.release_result = [](CordisAbiResult *result) {
    ++released_results;
    abi::detail::release(result);
  };
  const std::string opaque(1, static_cast<char>(255));
  for (auto policy : {cordis::Validation::Contract, cordis::Validation::Checked,
                      cordis::Validation::Audit}) {
    for (bool unknown : {false, true}) {
      auto result = abi::detail::result(abi::Payload(cordis::Message(opaque)), 17);
      if (unknown) {
        auto &bytes = static_cast<abi::detail::Output *>(result.owner)->bytes;
        bytes.insert(bytes.end(), {0x78, 1});
        result.value = {bytes.data(), bytes.size()};
      }
      auto before = released_results;
      bool accepted = false;
      try {
        auto reply = abi::detail::consume(&api, result, true, policy);
        assert(reply.second == 17 && reply.first.one().bytes() == opaque);
        accepted = true;
      } catch (const cordis::RpcError &) {
        throw std::logic_error("invalid carrier became a known business rejection");
      } catch (const std::runtime_error &) {
      }
      assert(accepted == (!unknown || policy != cordis::Validation::Audit));
      assert(released_results == before + 1);
    }
  }
}
int main() {
  failure_categories();
  process_contexts();
  abi_contexts();
  carrier_results();
}
