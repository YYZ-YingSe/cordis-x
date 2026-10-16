#include "google/protobuf/cordis_thread_storage.h"
#include <memory>
#include <mutex>
#include <pthread.h>
#include <stdexcept>
#include <vector>

namespace google::protobuf::internal {
namespace {
struct ThreadCells {
  std::vector<void *> values;
};
class Storage {
public:
  pthread_key_t key{};
  std::mutex mutex;
  std::vector<CordisThreadDelete> destructors;
  std::vector<std::unique_ptr<ThreadCells>> threads;

  Storage() {
    if (pthread_key_create(&key, nullptr))
      throw std::runtime_error("protobuf thread storage unavailable");
  }
  ~Storage() {
    // No protobuf call or cache access remains at this point. Key deletion
    // invalidates access via the key without installing any exit callback.
    pthread_key_delete(key);
    for (const auto &thread : threads)
      for (std::size_t i = 0; i < thread->values.size(); ++i)
        if (thread->values[i])
          destructors[i](thread->values[i]);
  }
};
Storage *&storage() {
  // Shutdown order is explicit: these cells must outlive every protobuf
  // shutdown callback. A function-local object's destructor runs too early.
  static auto *value = new Storage;
  return value;
}
} // namespace

std::size_t CordisRegisterThreadSlot(CordisThreadDelete destroy) {
  auto &state = *storage();
  std::lock_guard lock(state.mutex);
  const auto slot = state.destructors.size();
  state.destructors.push_back(destroy);
  return slot;
}
void *CordisThreadValue(std::size_t slot, void *(*create)(const void *),
                        const void *initial) {
  auto &state = *storage();
  auto *thread = static_cast<ThreadCells *>(pthread_getspecific(state.key));
  if (!thread) {
    auto owned = std::make_unique<ThreadCells>();
    std::lock_guard lock(state.mutex);
    state.threads.push_back(std::move(owned));
    thread = state.threads.back().get();
    if (pthread_setspecific(state.key, thread)) {
      state.threads.pop_back();
      throw std::runtime_error("protobuf thread storage unavailable");
    }
  }
  // Only this thread reads/writes its cells. The module's destructor runs
  // after all entries have returned and all instance workers have joined.
  if (thread->values.size() <= slot)
    thread->values.resize(slot + 1);
  if (!thread->values[slot])
    thread->values[slot] = create(initial);
  return thread->values[slot];
}
void CordisShutdownThreadStorage() {
  auto *&state = storage();
  delete state;
  state = nullptr;
}
} // namespace google::protobuf::internal
