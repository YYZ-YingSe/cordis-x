#include <cordis/examples/examples.hpp>
#include <cassert>
#include <cordis/adapters/process.hpp>
struct Adder final : example::AdderServer {
  static inline int created = 0;
  static std::shared_ptr<Adder> create(cordis::data::Unit) {
    ++created;
    return std::make_shared<Adder>();
  }
  cordis::Async<cordis::data::Int64> add(cordis::Context,
                                         example::AddArgs args) override {
    cordis::data::Int64 result;
    result.set_value(args.left() + args.right());
    co_return result;
  }
};
int main() {
  cordis::process::detail::ServiceBridge<example::AdderModel<Adder>> bridge(
      nullptr);
  assert(Adder::created == 0);
  (void)bridge.interface();
  (void)bridge.dependency_interfaces();
  assert(Adder::created == 0);
}
