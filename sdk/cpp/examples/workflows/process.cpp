#include "service.hpp"
#include <cordis/adapters/process.hpp>
int main(int argc, char **argv) {
  const bool stdio_only = argc == 2 && std::string(argv[1]) == "--stdio-only";
  if (argc > 1 && !stdio_only) return 2;
  return cordis::process::run<example::workflows::Model>(!stdio_only, cordis::Value::object());
}
