#include <cordis/examples/calculator_logging.hpp>
#include <cordis/examples/calculator_process.hpp>
#include <cordis/tests/remote_fixture.hpp>
#include <cordis/adapters/process.hpp>
#include <fstream>
#include <limits>
#ifdef CORDIS_PACKAGE_HELPER
#include "../tests/package/helper.hpp"
#define CORDIS_PROCESS_CODE_VERSION CORDIS_HELPER_SYMBOL()
#endif
#ifndef CORDIS_PROCESS_CODE_VERSION
#define CORDIS_PROCESS_CODE_VERSION "v1"
#endif
using namespace cordis;
using Number = google::protobuf::Int64Value;
using Text = google::protobuf::StringValue;
using Unit = google::protobuf::Empty;
static Number number(int64_t value) {
  Number result;
  result.set_value(value);
  return result;
}
static Text text(std::string value) {
  Text result;
  result.set_value(std::move(value));
  return result;
}
static int64_t sum(int64_t a, int64_t b) {
  if ((b > 0 && a > INT64_MAX - b) || (b < 0 && a < INT64_MIN - b))
    throw RpcError("overflow", "int64 addition overflow");
  return a + b;
}
struct Options {
  bool fail_start = false, fail_stop = false;
  std::string trace;
};
static Options options;
struct Ordinary {
  using ServiceServer = fixtures_calculator_process::ServiceServer;
  using ServiceCounterServer =
      fixtures_calculator_process::ServiceCounterServer;
  using AddArgs = fixtures::calculator_process::AddArgs;
  using CounterArgs = fixtures::calculator_process::CounterArgs;
  using MetadataResult = fixtures::calculator_process::MetadataResult;
  using MetadataForArgs = fixtures::calculator_process::MetadataForArgs;
  using MetadataForResult = fixtures::calculator_process::MetadataForResult;
  using CounterMetadataResult =
      fixtures::calculator_process::CounterMetadataResult;
  template <class T> using Model = fixtures_calculator_process::ServiceModel<T>;
};
struct Logging {
  using ServiceServer = fixtures_calculator_logging::ServiceServer;
  using ServiceCounterServer =
      fixtures_calculator_logging::ServiceCounterServer;
  using AddArgs = fixtures::calculator_logging::AddArgs;
  using CounterArgs = fixtures::calculator_logging::CounterArgs;
  using MetadataResult = fixtures::calculator_logging::MetadataResult;
  using MetadataForArgs = fixtures::calculator_logging::MetadataForArgs;
  using MetadataForResult = fixtures::calculator_logging::MetadataForResult;
  using CounterMetadataResult =
      fixtures::calculator_logging::CounterMetadataResult;
  template <class T> using Model = fixtures_calculator_logging::ServiceModel<T>;
};
struct Probe {
  using ServiceServer = fixtures_remote_fixture::ServiceServer;
  using ServiceCounterServer = fixtures_remote_fixture::ServiceCounterServer;
  using AddArgs = fixtures::remote_fixture::AddArgs;
  using CounterArgs = fixtures::remote_fixture::CounterArgs;
  using MetadataResult = fixtures::remote_fixture::MetadataResult;
  using MetadataForArgs = fixtures::remote_fixture::MetadataForArgs;
  using MetadataForResult = fixtures::remote_fixture::MetadataForResult;
  using CounterMetadataResult = fixtures::remote_fixture::CounterMetadataResult;
  template <class T> using Model = fixtures_remote_fixture::ServiceModel<T>;
};
template <class Profile>
class Calculator final : public Profile::ServiceServer {
  using AddArgs = typename Profile::AddArgs;
  using CounterArgs = typename Profile::CounterArgs;
  using MetadataResult = typename Profile::MetadataResult;
  using MetadataForArgs = typename Profile::MetadataForArgs;
  using MetadataForResult = typename Profile::MetadataForResult;
  using CounterMetadataResult = typename Profile::CounterMetadataResult;
  using CounterServer = typename Profile::ServiceCounterServer;
  std::mutex mutex;
  std::map<std::string, std::shared_ptr<corral::Event>> signals;
  std::set<std::string> waiting, released;
  bool open = false;
  void trace(const std::string &event) {
    if (!options.trace.empty()) {
      std::ofstream out(options.trace, std::ios::app);
      out << event << '\n';
    }
  }
  void ready() {
    if (!open)
      throw Refusal("closed", "calculator closed");
  }
  class Counter final : public CounterServer {
    Calculator &parent;
    int64_t value;
    std::string label;
    bool fail_close;

  public:
    Counter(Calculator &parent, int64_t value, std::string label,
            bool fail_close)
        : parent(parent), value(value), label(std::move(label)),
          fail_close(fail_close) {}
    Async<Number> get(Context, Unit) {
      std::lock_guard lock(parent.mutex);
      parent.ready();
      co_return number(value);
    }
    Async<Number> add(Context, Number args) {
      std::lock_guard lock(parent.mutex);
      parent.ready();
      value = sum(value, args.value());
      co_return number(value);
    }
    Async<Text> code_version(Context, Unit) {
      co_return text(CORDIS_PROCESS_CODE_VERSION);
    }
    Async<CounterMetadataResult> metadata(Context ctx, Unit) {
      co_return protobuf::decode<CounterMetadataResult>(
          co_await ctx.metadata("example.calculator", "1"));
    }
    Async<Text> hold(Context ctx, Text args) {
      co_return co_await parent.hold(ctx, std::move(args));
    }
    Async<Unit> release(Context ctx, Text args) {
      co_return co_await parent.release(ctx, std::move(args));
    }
    Async<Unit> crash(Context ctx, Unit args) {
      co_return co_await parent.crash(ctx, std::move(args));
    }
    Async<void> close(Context) {
      std::lock_guard lock(parent.mutex);
      if (!parent.open)
        throw std::runtime_error("provider closed before resource");
      parent.trace("resource_closed:" + label);
      if (fail_close)
        throw RpcError("release_failed", "injected resource cleanup failure");
      co_return;
    }
  };

public:
  static std::shared_ptr<Calculator> create(cordis::data::Unit) {
    return std::make_shared<Calculator>();
  }
  Async<void> activate(Context) {
    std::lock_guard lock(mutex);
    open = true;
    trace("activated");
    if (options.fail_start)
      throw RpcError("activate_failed", "injected startup failure");
    co_return;
  }
  Async<void> deactivate(Context) {
    std::lock_guard lock(mutex);
    if (!waiting.empty())
      throw std::runtime_error("deactivated while call was running");
    open = false;
    trace("deactivated");
    if (options.fail_stop)
      throw RpcError("deactivate_failed", "injected cleanup failure");
    co_return;
  }
  Async<Number> add(Context, AddArgs args) {
    std::lock_guard lock(mutex);
    ready();
    co_return number(sum(args.left(), args.right()));
  }
  Async<Text> code_version(Context, Unit) {
    std::lock_guard lock(mutex);
    ready();
    co_return text(CORDIS_PROCESS_CODE_VERSION);
  }
  Async<google::protobuf::Value> echo(Context, google::protobuf::Value args) {
    std::lock_guard lock(mutex);
    ready();
    co_return args;
  }
  Async<MetadataResult> metadata(Context ctx, Unit) {
    co_return protobuf::decode<MetadataResult>(
        co_await ctx.metadata("example.calculator", "1"));
  }
  Async<MetadataForResult> metadata_for(Context ctx, MetadataForArgs args) {
    co_return protobuf::decode<MetadataForResult>(
        co_await ctx.metadata(args.name(), args.revision()));
  }
  Async<Text> log_version(Context ctx, Unit) {
    co_return co_await fixtures_calculator_logging::ServiceDependencies::
        logger()
            .write(ctx, text(CORDIS_PROCESS_CODE_VERSION));
  }
  Async<std::shared_ptr<CounterServer>> counter(Context ctx, CounterArgs args) {
    std::string label = "counter";
    bool fail_close = false;
    if constexpr (std::is_same_v<Profile, Probe>) {
      if (args.has_name())
        label = args.name();
      fail_close = args.fail_close();
      if (args.has_wait())
        co_await hold(ctx, text(args.wait()));
    }
    std::lock_guard lock(mutex);
    ready();
    trace("resource_acquired:" + label);
    co_return std::make_shared<Counter>(*this, args.initial(), label,
                                        fail_close);
  }
  Async<Text> hold(Context, Text args) {
    const auto token = args.value();
    std::unique_lock lock(mutex);
    ready();
    waiting.insert(token);
    trace("hold:" + token);
    auto signal = std::make_shared<corral::Event>();
    signals[token] = signal;
    const bool done = released.count(token) != 0;
    lock.unlock();
    if (!done)
      co_await *signal;
    lock.lock();
    signals.erase(token);
    released.erase(token);
    waiting.erase(token);
    ready();
    trace("released:" + token);
    co_return args;
  }
  Async<Unit> release(Context, Text args) {
    std::unique_lock lock(mutex);
    released.insert(args.value());
    auto found = signals.find(args.value());
    auto signal = found == signals.end() ? nullptr : found->second;
    lock.unlock();
    if (signal)
      signal->trigger();
    co_return Unit{};
  }
  Async<fixtures::remote_fixture::StatusResult> status(Context, Unit) {
    std::lock_guard lock(mutex);
    fixtures::remote_fixture::StatusResult result;
    result.set_open(open);
    for (const auto &token : waiting)
      result.add_waiting(token);
    co_return result;
  }
  Async<Unit> crash(Context, Unit) {
    std::_Exit(70);
    co_return Unit{};
  }
};
int main(int argc, char **argv) {
  bool hooks = false, http = true, logging = false;
  for (int i = 1; i < argc; ++i) {
    const std::string arg = argv[i];
    if (arg == "--test-hooks")
      hooks = true;
    else if (arg == "--stdio-only")
      http = false;
    else if (arg == "--trace" && i + 1 < argc)
      options.trace = argv[++i];
    else if (arg == "--fail-activate")
      options.fail_start = true;
    else if (arg == "--fail-deactivate")
      options.fail_stop = true;
    else if (arg == "--logger")
      logging = true;
    else {
      std::cerr << "unknown option: " << arg << '\n';
      return 2;
    }
  }
  try {
    if (logging)
      return process::run<Logging::Model<Calculator<Logging>>>(http);
    if (hooks)
      return process::run<Probe::Model<Calculator<Probe>>>(http);
    return process::run<Ordinary::Model<Calculator<Ordinary>>>(http);
  } catch (const std::exception &error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
