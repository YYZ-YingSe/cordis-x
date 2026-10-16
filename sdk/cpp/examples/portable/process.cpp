#include "service.hpp"
#include <cordis/adapters/process.hpp>

int main(int argc, char **argv) {
  const bool http = argc < 2 || std::string(argv[1]) != "--stdio-only";
  return cordis::process::run<example::Model>(http);
}
