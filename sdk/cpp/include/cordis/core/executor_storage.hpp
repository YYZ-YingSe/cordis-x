#pragma once
#if defined(__APPLE__) && !defined(__wasm__)
#include <memory>
#include <mutex>
#include <pthread.h>
#include <stdexcept>
#include <unordered_set>

namespace corral {
class Executor;
}
namespace cordis::detail {
// Mach-O compiler TLS can keep a dylib mapped after its worker has exited.
// This storage uses a pthread key with no foreign-thread destructor callback.
// Workers explicitly retire their cells; other touched threads' small cells
// belong to the module and are freed when its last executor has been joined.
class ExecutorStorage {
  struct Cell {
    corral::Executor *slots[2]{};
  };
  pthread_key_t key_{};
  std::mutex mutex_;
  std::unordered_set<Cell *> cells_;

public:
  ExecutorStorage() {
    if (pthread_key_create(&key_, nullptr))
      throw std::runtime_error("executor storage unavailable");
  }
  ~ExecutorStorage() {
    pthread_key_delete(key_);
    for (auto *cell : cells_)
      delete cell;
  }
  corral::Executor *&get(unsigned index) {
    auto *cell = static_cast<Cell *>(pthread_getspecific(key_));
    if (!cell) {
      auto owned = std::make_unique<Cell>();
      std::lock_guard lock(mutex_);
      cells_.insert(owned.get());
      if (pthread_setspecific(key_, owned.get())) {
        cells_.erase(owned.get());
        throw std::runtime_error("executor storage unavailable");
      }
      cell = owned.release();
    }
    return cell->slots[index];
  }
  void retire() {
    auto *cell = static_cast<Cell *>(pthread_getspecific(key_));
    if (!cell)
      return;
    pthread_setspecific(key_, nullptr);
    std::lock_guard lock(mutex_);
    cells_.erase(cell);
    delete cell;
  }
};
// Initialize before function-local plugin registries so their instance workers
// are joined before this storage is destroyed at unload or process exit.
inline ExecutorStorage executor_storage;
} // namespace cordis::detail
#define CORRAL_EXECUTOR_STORAGE(index)                                         \
  ::cordis::detail::executor_storage.get(index)
#endif
