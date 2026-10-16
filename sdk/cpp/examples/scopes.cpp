#include "lifecycle/scopes.hpp"
#include <cordis/adapters/process.hpp>
int main(int argc, char **argv) {
  const std::set<std::string> flags(argv + 1, argv + argc);
  const bool http = !flags.contains("--stdio-only");
  return cordis::process::run<example::lifecycle::ConnectionsModel>(
      http, {{"failActivate", flags.contains("--fail-activate")}});
}
