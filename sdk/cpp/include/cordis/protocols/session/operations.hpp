#pragma once
#include <cordis/core/error.hpp>
#include <charconv>
#include <cstdint>
#include <limits>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <vector>

namespace cordis::session {
inline constexpr std::size_t slots_per_lane = 128;
inline constexpr std::size_t default_operation_slots = 16 * slots_per_lane;
inline constexpr std::size_t output_frames = 128 + 8 * 16 + 14 * 16;
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
  std::vector<OperationSlot> slots;
public:
  explicit OperationOutbox(std::size_t capacity = default_operation_slots) : slots(capacity) {}
  // Caller serializes access together with its pending request map.
  std::string allocate(std::optional<std::size_t> lane = {}) {
    const auto begin = lane ? *lane * slots_per_lane : 0;
    const auto end = lane ? std::min(slots.size(), begin + slots_per_lane) : slots.size();
    for (std::size_t n = begin; n < end; ++n) {
      auto &s = slots[n];
      if (!s.active && s.generation != std::numeric_limits<uint64_t>::max()) {
        s.active = true;
        return OperationId{n, ++s.generation}.str();
      }
    }
    throw Refusal("overloaded", "request slots full");
  }
  bool complete(const std::string &value) {
    auto id = OperationId::parse(value);
    if (!id || id->slot >= slots.size()) return false;
    auto &s = slots[id->slot];
    if (!s.active || s.generation != id->generation) return false;
    s.active = false;
    return true;
  }
};
class OperationInbox {
  struct State { std::mutex mutex; std::vector<OperationSlot> slots;
    explicit State(std::size_t count) : slots(count) {} };
  std::shared_ptr<State> state;
public:
  explicit OperationInbox(std::size_t capacity = default_operation_slots)
      : state(std::make_shared<State>(capacity)) {}
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
    if (!id || id->slot >= state->slots.size())
      throw Refusal("protocol", "invalid request identity");
    auto &s = state->slots[id->slot];
    if (s.active || id->generation <= s.generation)
      throw Refusal("duplicate_operation", "active or retired request identity");
    s = {id->generation, true};
    return Admission(state, id->slot);
  }
};
}
