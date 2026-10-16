#include "lifecycle/subscriptions.hpp"
#include <cordis/adapters/process.hpp>
int main(int argc, char **argv) {
  const std::set<std::string> flags(argv + 1, argv + argc);
  const bool http = !flags.contains("--stdio-only");
  if (flags.contains("--logger"))
    return cordis::process::run<example::lifecycle::AuditedSubscriptionsModel>(
        http, cordis::Value::object());
  return cordis::process::run<example::lifecycle::SubscriptionsModel>(
      http, cordis::Value::object());
}
