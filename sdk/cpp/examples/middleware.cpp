#include "lifecycle/middleware.hpp"
#include <cordis/adapters/process.hpp>
int main(int argc, char **argv) {
  const std::set<std::string> flags(argv + 1, argv + argc);
  return cordis::process::run<example::lifecycle::PipelineModel>(
      !flags.contains("--stdio-only"),
      {{"listen", flags.contains("--listen")},
       {"failActivate", flags.contains("--fail-activate")}});
}
