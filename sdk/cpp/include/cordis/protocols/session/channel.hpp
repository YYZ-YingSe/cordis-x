#pragma once
#include "binary.hpp"
#include <algorithm>
#include <cordis/core/error.hpp>
#include <cordis/runtime/native.hpp>
#include <functional>
#include <map>
#include <memory>
#include <mutex>

namespace cordis::session {

// Responses are pumped independently of business calls, including lifecycle
// calls.
class DependencyChannel {
  std::mutex mutex;
  struct Pending {
    std::function<void(native::Outcome<Payload>)> reply;
    int lane;
  };
  std::map<std::string, Pending> pending;
  std::function<void(std::vector<uint8_t>)> write;
  std::string session;
  Identity activation;
  std::uint64_t next = 0;
  std::uint64_t next_resource = 0;
  bool closed = false;
  bool cleanup_calls = false;
  bool metadata_enabled = false;

public:
  Validation validation = Validation::Checked;
  std::string resource_id() {
    std::lock_guard lock(mutex);
    if (closed)
      throw Refusal("closed", "dependency channel closed");
    if (next_resource >= 4096)
      throw Refusal("resources_exhausted", "dependency resource history full");
    return "r" + std::to_string(++next_resource);
  }
  void configure(const std::string &token, const Identity &identity,
                 std::function<void(std::vector<uint8_t>)> output) {
    std::lock_guard lock(mutex);
    session = token;
    activation = identity;
    write = std::move(output);
  }
  void enable_metadata(bool enabled) {
    std::lock_guard lock(mutex);
    metadata_enabled = enabled;
  }
  Async<Json> metadata(std::string context, std::string name,
                       std::string revision, bool cleanup) {
    {
      std::lock_guard lock(mutex);
      if (!metadata_enabled)
        throw Refusal("unsupported_metadata", "metadata not negotiated");
    }
    return request(
        cleanup ? "cleanup_metadata" : "context_metadata",
        {{"context", context}, {"name", name}, {"revision", revision}});
  }
  void reserve_cleanup(bool enabled) {
    std::lock_guard lock(mutex);
    cleanup_calls = enabled;
  }
  Async<Message> invoke(std::string context, std::string dependency,
                        std::string method, Message args,
                        bool cleanup = false) {
    bool reserved;
    {
      std::lock_guard lock(mutex);
      reserved = cleanup_calls;
    }
    auto result = co_await request_payload(
        cleanup && reserved ? "cleanup_call" : "dependency_call",
        Payload(Json{{"context", context},
                     {"dependency", dependency},
                     {"method", method}},
                std::move(args)));
    co_return result.one();
  }
  Async<Json> request(std::string command, Json value) {
    co_return (
        co_await request_payload(std::move(command), Payload(std::move(value))))
        .control_only();
  }
  Async<Payload> request_payload(std::string command, Payload payload) {
    return native::completed<Payload>(
        [this, command = std::move(command), payload = std::move(payload)](
            std::function<void(native::Outcome<Payload>)> reply) {
          std::function<void(std::vector<uint8_t>)> output;
          std::vector<uint8_t> bytes;
          std::string operation;
          {
            std::lock_guard lock(mutex);
            if (closed || !write)
              throw Refusal("closed", "dependency channel closed");
            const int lane =
                (command == "child_install" || command == "value_provide") ? 12
                : (command == "child_ready" || command == "child_settled") ? 10
                : command == "child_dispose"                               ? 11
                : command == "middleware_run"                              ? 8
                : command == "middleware_next"                             ? 9
                : command == "event_emit"                                  ? 6
                : command == "event_join"                                  ? 7
                : (command == "cleanup_call" || command == "cleanup_metadata" ||
                   command == "cleanup_resource_call")
                    ? 5
                : command == "dependency_resource_call"                     ? 13
                : (command == "sequence_ready" || command == "scope_ready") ? 2
                : (command == "effect_dispose" ||
                   command == "sequence_dispose" ||
                   command == "scope_dispose" || command == "event_dispose" ||
                   command == "middleware_dispose" ||
                   command == "dependency_resource_dispose")
                    ? 1
                : command == "task_join" ? 4
                : command == "scope_run" ? 3
                                         : 0;
            if (std::count_if(pending.begin(), pending.end(),
                              [lane](const auto &item) {
                                return item.second.lane == lane;
                              }) >= 16)
              throw Refusal("overloaded", "dependency call capacity reached");
            if (next >= 65536)
              throw Refusal("session_exhausted",
                            "dependency operation history full");
            operation = std::to_string(++next);
            Request request{Header{session, activation, operation}, command,
                            payload};
            bytes = encode(Frame(std::move(request)));
            output = write;
            pending.emplace(operation, Pending{std::move(reply), lane});
          }
          try {
            output(std::move(bytes));
          } catch (...) {
            std::function<void(native::Outcome<Payload>)> complete;
            {
              std::lock_guard lock(mutex);
              auto found = pending.find(operation);
              if (found != pending.end()) {
                complete = std::move(found->second.reply);
                pending.erase(found);
              }
            }
            if (complete)
              complete(std::current_exception());
          }
        });
  }
  void receive(const Response &response) {
    std::function<void(native::Outcome<Payload>)> complete;
    native::Outcome<Payload> result;
    {
      std::lock_guard lock(mutex);
      if (response.session != session || response.activation != activation)
        throw std::runtime_error("dependency response identity mismatch");
      auto found = pending.find(response.operation);
      if (found == pending.end())
        throw std::runtime_error("unsolicited dependency response");
      const auto &outcome = response.outcome;
      if (outcome.kind == Outcome::Kind::success)
        result = outcome.value;
      else if (outcome.kind == Outcome::Kind::refused)
        result =
            std::make_exception_ptr(Refusal(outcome.code, outcome.message));
      else if (outcome.kind == Outcome::Kind::failure)
        result =
            std::make_exception_ptr(RpcError(outcome.code, outcome.message));
      else
        result = std::make_exception_ptr(std::runtime_error(
            "outcome unknown: " + outcome.code + ": " + outcome.message));
      complete = std::move(found->second.reply);
      pending.erase(found);
    }
    complete(std::move(result));
  }
  void close() {
    std::map<std::string, Pending> lost;
    {
      std::lock_guard lock(mutex);
      closed = true;
      lost.swap(pending);
      write = {};
    }
    auto failure = std::make_exception_ptr(
        std::runtime_error("dependency result lost with channel"));
    for (auto &[_, entry] : lost)
      entry.reply(failure);
  }
};
} // namespace cordis::session
