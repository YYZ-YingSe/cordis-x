#include "service.hpp"
#include <cordis/adapters/process.hpp>

#include "../../tests/resource_probe_client.hpp"
int main(int argc, char **argv) {
  bool http = true, client = false, probe = false;
  for (int i = 1; i < argc; ++i) {
    http = http && std::string(argv[i]) != "--stdio-only";
    probe = probe || std::string(argv[i]) == "--probe";
    client = client || std::string(argv[i]) == "--client";
  }
  if (client && probe)
    return cordis::process::run<ProbeClientModel>(http);
  return client ? cordis::process::run<example::resources::ClientModel>(http)
                : cordis::process::run<example::resources::CounterModel>(http);
}
