#pragma once
#include <cordis/core/error.hpp>
#include <charconv>
#include <array>
#include <algorithm>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>
#include <unordered_map>

namespace cordis::session {
inline constexpr std::size_t reserved_capacity = 16;
struct OperationLayout {
  std::vector<std::size_t> forward, reverse;
  std::size_t incoming, output, connections;
  explicit OperationLayout(std::size_t ordinary)
      : forward(9, std::max(ordinary, reserved_capacity)),
        reverse(14, std::max(ordinary, reserved_capacity)) {
    const auto auxiliary = reverse.front();
    if (ordinary == 0 || ordinary > UINT32_MAX || auxiliary > (UINT32_MAX - ordinary) / 22)
      throw Refusal("invalid_argument", "max_inflight must be positive and representable");
    forward[0] = ordinary;
    incoming = ordinary + 8 * auxiliary;
    output = incoming + 14 * auxiliary;
    connections = 2 * (ordinary + auxiliary);
  }
};
struct OperationId {
  std::size_t slot;
  uint64_t generation;
  std::string str() const { return std::to_string(slot) + ":" + std::to_string(generation); }
  static std::optional<OperationId> parse(const std::string &s) {
    const auto split = s.find(':');
    if (split == std::string::npos) return {};
    OperationId id{};
    auto a = std::from_chars(s.data(), s.data() + split, id.slot);
    auto b = std::from_chars(s.data() + split + 1, s.data() + s.size(), id.generation);
    if (a.ec != std::errc{} || a.ptr != s.data() + split || b.ec != std::errc{} ||
        b.ptr != s.data() + s.size() || !id.generation || id.str() != s) return {};
    return id;
  }
};
struct OperationSlot { uint64_t generation = 0; bool active = false; };
class OperationOutbox {
  struct Lane {
    std::size_t start, capacity;
    std::vector<OperationSlot> entries;
    std::vector<std::size_t> free;
  };
  std::vector<Lane> lanes;
public:
  explicit OperationOutbox(std::size_t capacity) : OperationOutbox(std::vector<std::size_t>{capacity}) {}
  explicit OperationOutbox(const std::vector<std::size_t> &capacities) {
    std::size_t end = 0;
    for (auto capacity : capacities) {
      if (capacity > std::numeric_limits<std::size_t>::max() - end)
        throw std::length_error("operation capacity overflow");
      lanes.push_back({end, capacity, {}, {}});
      end += capacity;
    }
  }
  // Caller serializes allocation with its pending request map.
  std::string allocate(std::optional<std::size_t> lane = {}) {
    for (std::size_t i = 0; i < lanes.size(); ++i) {
      if (lane && i != *lane) continue;
      auto &pool = lanes[i];
      std::size_t index;
      if (!pool.free.empty()) {
        index = pool.free.back();
        pool.free.pop_back();
      } else {
        if (pool.entries.size() >= pool.capacity) continue;
        index = pool.entries.size();
        pool.entries.emplace_back();
      }
      auto &entry = pool.entries[index];
      entry.active = true;
      return OperationId{pool.start + index, ++entry.generation}.str();
    }
    throw Refusal("overloaded", "request slots full");
  }
  bool complete(const std::string &value) {
    auto id = OperationId::parse(value);
    if (!id) return false;
    for (auto &pool : lanes) {
      if (id->slot < pool.start || id->slot - pool.start >= pool.entries.size()) continue;
      auto index = id->slot - pool.start;
      auto &entry = pool.entries[index];
      if (!entry.active || entry.generation != id->generation) return false;
      entry.active = false;
      if (entry.generation != UINT64_MAX) pool.free.push_back(index);
      return true;
    }
    return false;
  }
};
class OperationInbox {
  struct State { std::mutex mutex; std::size_t capacity;
    std::unordered_map<std::size_t, OperationSlot> slots;
    explicit State(std::size_t count) : capacity(count) {} };
  std::shared_ptr<State> state;
public:
  explicit OperationInbox(std::size_t capacity)
      : state(std::make_shared<State>(capacity)) {}
  void grow(std::size_t capacity) {
    std::lock_guard lock(state->mutex);
    if (capacity < state->capacity) throw std::length_error("operation table cannot shrink");
    state->capacity = capacity;
  }
  struct Admission {
    std::shared_ptr<State> state;
    std::size_t slot;
    Admission(std::shared_ptr<State> s, std::size_t n) : state(std::move(s)), slot(n) {}
    Admission(const Admission &) = delete;
    ~Admission() { std::lock_guard lock(state->mutex); state->slots[slot].active = false; }
  };
  Admission admit(const std::string &value) {
    auto id = OperationId::parse(value);
    std::lock_guard lock(state->mutex);
    if (!id || id->slot >= state->capacity)
      throw Refusal("protocol", "invalid request identity");
    auto &s = state->slots[id->slot];
    if (s.active || id->generation <= s.generation)
      throw Refusal("duplicate_operation", "active or retired request identity");
    s = {id->generation, true};
    return Admission(state, id->slot);
  }
};
}
