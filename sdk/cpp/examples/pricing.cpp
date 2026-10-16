#include <cordis/examples/pricing.hpp>
#include <cordis/adapters/process.hpp>

class Pricing final : public fixtures_pricing::PricingServer {
  static cordis::Async<fixtures::logger::WriteResult> log(cordis::Context ctx,
                                                          std::string event) {
    google::protobuf::StringValue args;
    args.set_value(std::move(event));
    co_return co_await fixtures_pricing::PricingDependencies::logger().write(
        std::move(ctx), std::move(args));
  }

public:
  static std::shared_ptr<Pricing> create(cordis::data::Unit) {
    return std::make_shared<Pricing>();
  }
  cordis::Async<void> activate(cordis::Context ctx) override {
    co_await log(std::move(ctx), "pricing.started");
  }
  cordis::Async<void> deactivate(cordis::Context ctx) override {
    co_await log(std::move(ctx), "pricing.stopped");
  }
  cordis::Async<fixtures::pricing::FlushResult>
  flush(cordis::Context ctx, google::protobuf::Empty) override {
    auto receipt = co_await log(std::move(ctx), "pricing.flushed");
    fixtures::pricing::FlushResult result;
    result.set_generation(receipt.generation());
    result.set_owner(receipt.owner());
    result.set_caller(receipt.caller());
    co_return result;
  }
  cordis::Async<fixtures::pricing::QuoteResult>
  quote(cordis::Context ctx, fixtures::pricing::QuoteArgs args) override {
    if (args.quantity() < 0 || args.quantity() > 1'000'000 ||
        args.unit_price() < 0 || args.unit_price() > 1'000'000)
      throw fixtures_pricing::failure(
          fixtures_pricing::PricingQuoteError::InvalidArgument,
          "quantity and unit_price must be in [0, 1000000]");
    auto receipt = co_await log(std::move(ctx), "pricing.quoted");
    fixtures::pricing::QuoteResult result;
    result.set_total(args.quantity() * args.unit_price());
    result.mutable_receipt()->set_generation(receipt.generation());
    result.mutable_receipt()->set_owner(receipt.owner());
    result.mutable_receipt()->set_caller(receipt.caller());
    co_return result;
  }
};
int main(int argc, char **argv) {
  return cordis::process::run<fixtures_pricing::PricingModel<Pricing>>(
      !(argc == 2 && std::string(argv[1]) == "--stdio-only"),
      cordis::Value::object());
}
