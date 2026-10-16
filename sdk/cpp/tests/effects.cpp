#include "binary.hpp"
#include "executor.hpp"
#include <condition_variable>
#include <cordis/adapters/process/host.hpp>
#include <fstream>

static void check_at(bool ok, int line) {
  if (!ok)
    throw std::runtime_error("effect regression at line " +
                             std::to_string(line));
}
#define check(ok) check_at((ok), __LINE__)
using Peer = test::Peer;
static void regression() {
  TestExecutor executor;
  Peer peer;
  auto effects = std::make_shared<cordis::process::EffectRegistry>();
  cordis::process::Context ctx(peer.channel, std::string(32, 'b'),
                               cordis::process::Json::object(), effects);
  std::atomic<int> releases{0};
  auto registering = executor.start([&] {
    return ctx.defer("subscription", [&](auto) -> cordis::Async<void> {
      ++releases;
      co_return;
    });
  });
  auto request = peer.next();
  check(request.command == "effect_register");
  cordis::process::Context cleanup(peer.channel, std::string(32, 'c'),
                                   cordis::process::Json::object(), effects,
                                   true);
  try {
    executor.wait(cleanup.defer(
        "nested", [](auto) -> cordis::Async<void> { co_return; }));
    check(false);
  } catch (const cordis::process::RpcError &error) {
    check(error.code == "closed");
  }
  // A cleanup request may arrive before the registering callback receives its
  // reply.
  executor.wait(effects->release("e1", cleanup));
  peer.reply(request, {{"effect", "e1"}, {"grant", std::string(32, 'f')}});
  auto effect = registering.get();
  executor.wait(ctx.finish());
  try {
    executor.wait(
        ctx.defer("late", [](auto) -> cordis::Async<void> { co_return; }));
    check(false);
  } catch (const cordis::process::RpcError &error) {
    check(error.code == "closed");
  }
  auto disposing = executor.start([&] { return effect.dispose(); });
  request = peer.next();
  check(request.command == "effect_dispose");
  check(!request.payload.control.contains("context"));
  executor.wait(effects->release("e1", cleanup));
  peer.reply(request, cordis::process::Json::object());
  disposing.get();
  check(releases == 1 && effects->released());

  std::promise<void> entered;
  corral::Event proceed;
  cordis::process::Context failing(peer.channel, std::string(32, 'd'),
                                   cordis::process::Json::object(), effects);
  auto failed_registration = executor.start([&] {
    return failing.defer("failed", [&](auto) -> cordis::Async<void> {
      ++releases;
      entered.set_value();
      co_await proceed;
      throw cordis::process::RpcError("cleanup_failed", "failed inverse");

      co_return;
    });
  });
  request = peer.next();
  peer.reply(request, {{"effect", "e2"}, {"grant", std::string(32, 'e')}});
  failed_registration.get();
  executor.wait(failing.finish());
  auto release = [&]() -> cordis::Async<void> {
    cordis::process::Context context(peer.channel, std::string(32, 'c'),
                                     cordis::process::Json::object(), effects,
                                     true);
    try {
      (co_await effects->release("e2", context));
      check(false);
    } catch (const cordis::process::RpcError &error) {
      check(error.code == "cleanup_failed");
    }

    co_return;
  };
  auto first = executor.start(release);
  entered.get_future().get();
  auto second = executor.start(release);
  check(!effects->released());
  executor.runtime.post([&] { proceed.trigger(); });
  first.get();
  second.get();
  check(releases == 2 && !effects->released());

  auto unknown = std::make_shared<cordis::process::EffectRegistry>();
  cordis::process::Context unconfirmed(peer.channel, std::string(32, 'd'),
                                       cordis::process::Json::object(),
                                       unknown);
  auto waiting = executor.start([&] {
    return unconfirmed.defer("unknown",
                             [](auto) -> cordis::Async<void> { co_return; });
  });
  peer.next();
  peer.channel->close();
  try {
    waiting.get();
    check(false);
  } catch (const std::runtime_error &error) {
    check(dynamic_cast<const cordis::RpcError *>(&error) == nullptr);
  }
  executor.wait(unconfirmed.finish());
  check(!unknown->released());
}

static void sequence_regression() {
  TestExecutor executor;
  Peer peer;
  auto sequences = std::make_shared<cordis::process::SequenceRegistry>();
  auto context = [&] {
    return cordis::process::Context(peer.channel, std::string(32, 'b'),
                                    {{"logger", "1"}}, nullptr, false,
                                    sequences);
  };
  auto ctx = context();
  std::optional<cordis::process::Context> saved;
  std::vector<int> undos;
  auto registering = executor.start([&] {
    return ctx.effect(
        "sequence",
        [&, n = 0](cordis::process::Context step) mutable
            -> cordis::Async<cordis::process::EffectStep> {
          ++n;
          if (n == 1)
            saved = step;
          else {
            try {
              (co_await saved->invoke("logger", "write", test::body(nullptr)));
              check(false);
            } catch (const cordis::process::RpcError &error) {
              check(error.code == "closed");
            }
          }
          cordis::process::Inverse inverse{"step",
                                           [&, n](auto) -> cordis::Async<void> {
                                             undos.push_back(n);
                                             co_return;
                                           }};
          co_return n == 2
              ? cordis::process::EffectStep::complete(std::move(inverse))
              : cordis::process::EffectStep::yielded(std::move(inverse));
        });
  });
  auto request = peer.next();
  peer.reply(request, {{"sequence", "s1"}, {"grant", std::string(32, 'f')}});
  auto sequence = registering.get();
  executor.wait(ctx.finish());
  try {
    executor.wait(ctx.effect(
        "late", [](auto) -> cordis::Async<cordis::process::EffectStep> {
          co_return cordis::process::EffectStep::complete();
        }));
    check(false);
  } catch (const cordis::process::RpcError &error) {
    check(error.code == "closed");
  }
  try {
    executor.wait(sequences->advance("s1", "2", context()));
    check(false);
  } catch (const cordis::process::RpcError &error) {
    check(error.code == "invalid_step");
  }
  check(executor.wait(sequences->advance("s1", "1", context())).at("done") ==
        false);
  const auto last = executor.wait(sequences->advance("s1", "2", context()));
  check(last.at("done") == true && last.at("inverse") == "step");
  try {
    sequences->release("s1");
    check(false);
  } catch (const cordis::process::RpcError &error) {
    check(error.code == "effects_remaining");
  }
  executor.wait(sequences->undo("s1", "2", context()));
  executor.wait(sequences->undo("s1", "2", context()));
  executor.wait(sequences->undo("s1", "1", context()));
  sequences->release("s1");
  check(sequences->released() && undos == std::vector<int>({2, 1}));
  auto waiting = executor.start([&] { return sequence.ready(); });
  request = peer.next();
  check(request.command == "sequence_ready");
  peer.reply(request, {{"completion", "completed"}});
  check(waiting.get() == "completed");
}

static void scope_regression() {
  TestExecutor executor;
  Peer peer;
  auto scopes = std::make_shared<cordis::process::ScopeRegistry>();
  auto context = [&] {
    return cordis::process::Context(peer.channel, std::string(32, 'b'),
                                    {{"logger", "1"}}, nullptr, false, nullptr,
                                    scopes);
  };
  auto ctx = context();
  auto registering = executor.start([&] { return ctx.scope("connection"); });
  auto request = peer.next();
  peer.reply(request, {{"scope", "c1"}, {"grant", std::string(32, 'f')}});
  auto scope = registering.get();
  executor.wait(ctx.finish());
  try {
    executor.wait(ctx.scope("expired"));
    check(false);
  } catch (const cordis::process::RpcError &e) {
    check(e.code == "closed");
  }
  std::optional<cordis::process::Context> saved;
  int calls = 0;
  auto running = executor.start([&] {
    return scope.run([&](cordis::process::Context child)
                         -> cordis::Async<cordis::process::Json> {
      saved = child;
      ++calls;
      throw cordis::process::RpcError("closed", "business error");
    });
  });
  request = peer.next();
  check(request.command == "scope_run");
  try {
    executor.wait(scopes->invoke("c2", "u1", context()));
    check(false);
  } catch (const cordis::process::RpcError &e) {
    check(e.code == "closed");
  }
  auto outcome = executor.wait(scopes->invoke("c1", "u1", context()));
  check(outcome.at("kind") == "failure" &&
        outcome.at("error").at("message") == "business error");
  try {
    executor.wait(saved->invoke("logger", "write", test::body(nullptr)));
    check(false);
  } catch (const cordis::process::RpcError &e) {
    check(e.code == "closed");
  }
  try {
    executor.wait(scopes->invoke("c1", "u1", context()));
    check(false);
  } catch (const cordis::process::RpcError &e) {
    check(e.code == "closed");
  }
  peer.reply(request, nullptr);
  running.get();
  check(calls == 1 && scopes->idle());
  auto disposing = executor.start([&] { return scope.dispose(); });
  request = peer.next();
  check(request.command == "scope_dispose" &&
        !request.payload.control.contains("context"));
  peer.reply(request, cordis::process::Json::object());
  disposing.get();
  cordis::process::Context cleanup(peer.channel, std::string(32, 'c'),
                                   cordis::process::Json::object(), nullptr,
                                   true, nullptr, scopes);
  try {
    executor.wait(cleanup.scope("forbidden"));
    check(false);
  } catch (const cordis::process::RpcError &e) {
    check(e.code == "closed");
  }
}

static void task_regression() {
  TestExecutor executor;
  Peer peer;
  auto tasks = std::make_shared<cordis::process::TaskRegistry>();
  cordis::process::Context ctx(peer.channel, std::string(32, 'b'),
                               {{"logger", "1"}}, nullptr, false, nullptr,
                               nullptr, tasks);
  std::optional<cordis::process::Context> saved;
  int calls = 0;
  auto registration = executor.start([&] {
    return ctx.spawn(
        "worker", [&](cordis::process::Context worker) -> cordis::Async<void> {
          saved = worker;
          co_await worker.cancelled();
          ++calls;

          co_return;
        });
  });
  auto request = peer.next();
  tasks->cancel("t1");
  tasks->cancel("t1");
  cordis::process::Context worker(peer.channel, std::string(32, 'c'),
                                  {{"logger", "1"}}, nullptr, false, nullptr,
                                  nullptr, tasks, tasks->cancellation("t1"));
  check(executor.wait(tasks->invoke("t1", worker)).at("kind") == "success");
  peer.reply(request, {{"task", "t1"}, {"grant", std::string(32, 'f')}});
  auto task = registration.get();
  executor.wait(ctx.finish());
  try {
    executor.wait(saved->invoke("logger", "write", test::body(nullptr)));
    check(false);
  } catch (const cordis::process::RpcError &error) {
    check(error.code == "closed");
  }
  try {
    executor.wait(tasks->invoke("t1", worker));
    check(false);
  } catch (const cordis::process::RpcError &error) {
    check(error.code == "closed");
  }
  auto join = executor.start([&] { return task.join(); });
  request = peer.next();
  check(request.command == "task_join" &&
        !request.payload.control.contains("context"));
  peer.reply(request, nullptr);
  join.get();
  check(calls == 1 && tasks->idle());
  cordis::process::Context cleanup(peer.channel, std::string(32, 'd'),
                                   {{"logger", "1"}}, nullptr, true, nullptr,
                                   nullptr, tasks);
  try {
    executor.wait(cleanup.spawn(
        "forbidden", [](auto) -> cordis::Async<void> { co_return; }));
    check(false);
  } catch (const cordis::process::RpcError &error) {
    check(error.code == "closed");
  }
  peer.channel->reserve_cleanup(true);
  auto cleaning = executor.start(
      [&] { return cleanup.invoke("logger", "write", test::body(nullptr)); });
  request = peer.next();
  check(request.command == "cleanup_call");
  peer.reply(request, nullptr);
  cleaning.get();
}

static void event_regression() {
  TestExecutor executor;
  Peer peer;
  auto events =
      std::make_shared<cordis::process::EventRegistry>(cordis::process::Json{
          {"tick", {{"name", "example.tick"}, {"revision", "1"}}}});
  auto context = [&] {
    return cordis::process::Context(peer.channel, std::string(32, 'b'),
                                    {{"logger", "1"}}, nullptr, false, nullptr,
                                    nullptr, nullptr, {}, events);
  };
  auto original = context();
  std::optional<cordis::process::Context> saved;
  auto registration = executor.start([&] {
    return original.on("tick",
                       [&](cordis::Message args, cordis::process::Context ctx)
                           -> cordis::Async<cordis::Flow> {
                         saved = ctx;
                         (co_await ctx.invoke("logger", "write", args));
                         co_return cordis::process::Break(args);
                       });
  });
  auto request = peer.next();
  auto callback = executor.start(
      [&] { return events->invoke("l1", "v1", test::body(7), context()); });
  auto dependency = peer.next();
  try {
    events->release("l1");
    check(false);
  } catch (const cordis::process::RpcError &e) {
    check(e.code == "listeners_running");
  }
  peer.reply(dependency, nullptr);
  auto event_result = callback.get();
  check((event_result.control == cordis::Value{{"kind", "break"}}));
  check(test::json(event_result.one()) == 7);
  peer.reply(request, {{"listener", "l1"}, {"grant", std::string(32, 'f')}});
  auto subscription = registration.get();
  executor.wait(original.finish());
  try {
    executor.wait(saved->invoke("logger", "write", test::body(nullptr)));
    check(false);
  } catch (const cordis::process::RpcError &e) {
    check(e.code == "closed");
  }
  try {
    executor.wait(events->invoke("l1", "v1", test::body(nullptr), context()));
    check(false);
  } catch (const cordis::process::RpcError &e) {
    check(e.code == "closed");
  }
  auto disposing = executor.start([&] { return subscription.dispose(); });
  request = peer.next();
  check(request.command == "event_dispose");
  events->release("l1");
  events->release("l1");
  peer.reply(request, cordis::process::Json::object());
  disposing.get();
  check(events->released());
  std::vector<std::future<cordis::process::Json>> waits;
  std::vector<cordis::session::Request> requests;
  for (int n = 0; n < 16; ++n) {
    waits.push_back(executor.start([&] {
      return peer.channel->request("event_join",
                                   cordis::process::Json::object());
    }));
    requests.push_back(peer.next());
  }
  auto emission = executor.start(
      [&] { return context().emit("tick", test::body(nullptr)); });
  request = peer.next();
  check(request.command == "event_emit");
  peer.reply(request, {{"dispatch", "d1"}, {"grant", std::string(32, 'e')}});
  auto dispatch = emission.get();
  for (auto req : requests)
    peer.reply(req, nullptr);
  for (auto &wait : waits)
    wait.get();
  auto joining = executor.start([&] { return dispatch.join(); });
  request = peer.next();
  peer.reply(
      request,
      {{"kind", "failure"},
       {"errors", cordis::process::Json::array({{{"code", "listener_failed"},
                                                 {"message", "failure"}}})}});
  try {
    joining.get();
    check(false);
  } catch (const cordis::process::EventDispatchError &e) {
    check(e.errors.size() == 1);
  }
}

struct FailingStartup final : cordis::process::Service {
  std::string trace;
  bool sequence;
  explicit FailingStartup(std::string trace, bool sequence = false)
      : trace(std::move(trace)), sequence(sequence) {}
  void record(const char *event) {
    std::ofstream(trace, std::ios::app) << event << '\n';
  }
  cordis::process::Json interface() const override {
    return cordis::contract::value_interface(true);
  }
  cordis::process::Json contract() const override {
    return {{"name", "example.subscriptions"}, {"revision", "1"}};
  }
  cordis::Async<void> activate(cordis::process::Context ctx) override {
    if (sequence) {
      (co_await ctx.effect(
          "startup",
          [this](auto) -> cordis::Async<cordis::process::EffectStep> {
            record("step");
            co_return cordis::process::EffectStep::complete(
                cordis::process::Inverse{"startup",
                                         [this](auto) -> cordis::Async<void> {
                                           record("released");
                                           co_return;
                                         }});
          }));
    } else
      (co_await ctx.defer("startup", [this](auto) -> cordis::Async<void> {
        record("released");
        co_return;
      }));
    throw cordis::process::RpcError("activation_failed",
                                    "after effect registration");

    co_return;
  }
  cordis::Async<void> deactivate() override {
    record("deactivated");
    co_return;
  }
};
int main(int argc, char **argv) {
  if (argc >= 2) {
    FailingStartup service(argv[1], argc == 3);
    cordis::process::Host host(service, false);
    return host.run();
  }
  regression();
  sequence_regression();
  scope_regression();
  task_regression();
  event_regression();
}
