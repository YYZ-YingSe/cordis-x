// Exercise real generated protobuf messages with foreign threads that outlive
// dlclose. This host deliberately links neither Cordis nor protobuf/Abseil.
#include <cordis/tests/empty_config.hpp>
#include <cordis/abi.h>
#include <dlfcn.h>
#include <exception>
#include <iostream>
#include <latch>
#if defined(__APPLE__)
#include <mach-o/dyld.h>
#else
#include <link.h>
#endif
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

static void require(bool ok, const char *message) {
  if (!ok)
    throw std::runtime_error(message);
}
struct Module {
  void *first, *second;
  CordisAbiApi api{};
  explicit Module(const char *path)
      : first(dlopen(path, RTLD_NOW | RTLD_LOCAL)),
        second(dlopen(path, RTLD_NOW | RTLD_LOCAL)) {
    if (!first || !second) {
      const auto *error = dlerror();
      throw std::runtime_error(error ? error : "dlopen failed");
    }
    auto entry = reinterpret_cast<decltype(&cordis_abi_v1)>(
        dlsym(first, "cordis_abi_v1"));
    require(entry && entry(CORDIS_ABI_VERSION, &api, sizeof api) == 0,
            "invalid entry");
  }
  void exercise() const {
    auto description = api.describe();
    auto status = description.status;
    api.release_result(&description);
    require(status == CORDIS_ABI_OK, "describe failed");
    CordisAbiBuffer config{empty_config, sizeof empty_config};
    auto created = api.create(&config);
    auto instance = created.handle;
    status = created.status;
    api.release_result(&created);
    require(status == CORDIS_ABI_OK && instance, "create failed");
    auto destroyed = api.destroy(instance);
    status = destroyed.status;
    api.release_result(&destroyed);
    require(status == CORDIS_ABI_OK, "destroy failed");
  }
};
static bool resident(const char *path) {
  auto *handle = dlopen(path, RTLD_NOW | RTLD_NOLOAD);
  if (!handle)
    return false;
  dlclose(handle);
  return true;
}
static unsigned abseil_images() {
  unsigned count = 0;
#if defined(__APPLE__)
  for (uint32_t i = 0; i < _dyld_image_count(); ++i)
    if (std::string(_dyld_get_image_name(i)).find("libabseil_dll.") !=
        std::string::npos)
      ++count;
#else
  dl_iterate_phdr(
      [](dl_phdr_info *info, size_t, void *data) {
        if (std::string(info->dlpi_name).find("libabseil_dll.so.") !=
            std::string::npos)
          ++*static_cast<unsigned *>(data);
        return 0;
      },
      &count);
#endif
  return count;
}
int main(int argc, char **argv) {
  try {
    require(argc > 1, "generated plugin paths required");
    for (int round = 0; round < 20; ++round) {
      std::vector<Module> modules;
      for (int i = 1; i < argc; ++i)
        modules.emplace_back(argv[i]);
      require(abseil_images() == 1, "expected one shared Abseil runtime");
      constexpr int workers = 4;
      std::latch finished(workers), leave(1);
      std::vector<std::thread> threads;
      std::exception_ptr failures[workers]{};
      for (int i = 0; i < workers; ++i)
        threads.emplace_back([&, i] {
          try {
            for (const auto &module : modules)
              module.exercise();
          } catch (...) {
            failures[i] = std::current_exception();
          }
          finished.count_down();
          leave.wait();
        });
      finished.wait();
      bool held = true, unloaded = true;
      for (std::size_t i = 0; i < modules.size(); ++i) {
        dlclose(modules[i].first);
        held &= resident(argv[i + 1]);
        dlclose(modules[i].second);
        unloaded &= !resident(argv[i + 1]);
      }
      // Stale pthread destructors would jump into unmapped code here.
      leave.count_down();
      for (auto &thread : threads)
        thread.join();
      for (const auto &failure : failures)
        if (failure)
          std::rethrow_exception(failure);
      require(held, "module unloaded before the last handle");
      require(unloaded, "generated module remained mapped after dlclose");
      require(abseil_images() == 1, "Abseil must outlive plugin generations");
    }
    std::cout << "20 reload rounds, 4 surviving foreign threads, " << argc - 1
              << " generated modules: physically unloaded\n";
  } catch (const std::exception &error) {
    std::cerr << error.what() << '\n';
    return 1;
  }
}
