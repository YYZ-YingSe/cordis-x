#include "../message.hpp"
#include <cordis/adapters/component.hpp>

namespace probe {
using namespace cordis;
namespace backend = cordis::component::detail;
class Service final : public cordis::Service {
  std::optional<Task> task;
  unsigned completed = 0;
public:
  Async<Message> invoke(Context ctx, std::string method, Message) override {
    if (method == "race") {
      task = co_await ctx.spawn("observed task", [this](Context ctx) -> Async<void> {
        co_await ctx.cancelled_for(std::chrono::seconds(2));
        ++completed;
      });
      // Canonical import withdrawal must not wait for the two-second task,
      // nor destroy its callback or stop its owner-managed execution.
      auto timer = backend::request(backend::current()->shared_from_this(), "wait",
                                   Value{{"kind", "delay"}, {"milliseconds", 1}});
      co_await corral::anyOf(task->join(), std::move(timer));
      if (completed) throw std::runtime_error("task completed before observation withdrawal");
    } else if (method == "again") {
      co_await task->join();
      co_await task->join();
      if (completed != 1) throw std::runtime_error("task execution changed after withdrawal");
    } else {
      throw Refusal("unknown_method", method);
    }
    co_return test::body(true);
  }
};
struct Model {
  static Value interface() { return contract::value_interface(false); }
  static std::string contract_name() { return "test.cpp.observations"; }
  static std::string contract_revision() { return "1"; }
  static Value dependencies() { return Value::object(); }
  static Value dependency_interfaces() { return Value::object(); }
  static std::shared_ptr<cordis::Service> create(const Value &) { return std::make_shared<Service>(); }
};
}
CORDIS_COMPONENT_PLUGIN(probe::Model)
