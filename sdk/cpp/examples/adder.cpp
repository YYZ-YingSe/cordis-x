#include <cordis/examples/examples.hpp>
#include <cordis/adapters/process.hpp>
#include <limits>
class Adder final : public example::AdderServer {
public:
  static std::shared_ptr<Adder> create(cordis::data::Unit) {
    return std::make_shared<Adder>();
  }
  cordis::Async<cordis::data::Int64> add(cordis::Context,
                                         example::AddArgs args) override {
    const auto a = args.left(), b = args.right();
    if ((b > 0 && a > INT64_MAX - b) || (b < 0 && a < INT64_MIN - b))
      throw example::failure(example::AdderAddError::Overflow,
                             "int64 addition overflow");
    cordis::data::Int64 result;
    result.set_value(a + b);
    co_return result;
  }
};
int main(int argc, char **argv) {
  const bool stdio_only = argc == 2 && std::string(argv[1]) == "--stdio-only";
  if (argc > 1 && !stdio_only)
    return 2;
  return cordis::process::run<example::AdderModel<Adder>>(
      !stdio_only, cordis::Value::object());
}
