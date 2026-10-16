#include "lifecycle/composition.hpp"
#include <cordis/adapters/process.hpp>
int main(int argc, char **argv) {
  const std::set<std::string> flags(argv + 1, argv + argc);
  return cordis::process::run<example::lifecycle::ComposerModel>(
      !flags.contains("--stdio-only"),
      {{"publication", flags.contains("--missing")     ? "OMIT"
                       : flags.contains("--duplicate") ? "DUPLICATE"
                                                       : "NORMAL"},
       {"failActivate", flags.contains("--fail")}});
}
