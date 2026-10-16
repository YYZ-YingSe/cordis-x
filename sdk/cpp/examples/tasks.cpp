#include "lifecycle/tasks.hpp"
#include <cordis/adapters/process.hpp>
int main(int argc, char **argv) {
  const std::set<std::string> flags(argv + 1, argv + argc);
  const bool http = !flags.contains("--stdio-only");
  return cordis::process::run<example::lifecycle::HeartbeatsModel>(
      http, {{"mode", flags.contains("--task-fail")     ? "FAIL"
                      : flags.contains("--task-finite") ? "FINITE"
                                                        : "HEARTBEAT"},
             {"direct", flags.contains("--task-direct")},
             {"failActivate", flags.contains("--fail-activate")}});
}
