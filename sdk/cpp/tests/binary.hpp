#pragma once
#include "message.hpp"
#include <condition_variable>
#include <cordis/adapters/process/context.hpp>
#include <deque>
namespace test {
struct Peer {
  std::mutex mutex;
  std::condition_variable changed;
  std::deque<cordis::session::Request> requests;
  std::shared_ptr<cordis::session::DependencyChannel> channel =
      std::make_shared<cordis::session::DependencyChannel>();
  Peer() {
    channel->configure(
        std::string(32, 'a'), {"1", "2", "3"}, [this](const auto &bytes) {
          auto frame = cordis::session::decode(bytes);
          std::lock_guard lock(mutex);
          requests.push_back(
              std::get<cordis::session::Request>(std::move(frame)));
          changed.notify_all();
        });
  }
  cordis::session::Request next() {
    std::unique_lock lock(mutex);
    changed.wait(lock, [this] { return !requests.empty(); });
    auto request = std::move(requests.front());
    requests.pop_front();
    return request;
  }
  void reply(const cordis::session::Request &request, cordis::Value value) {
    using namespace cordis::session;
    auto payload = request.payload.messages.empty() ||
                           (request.command != "dependency_call" &&
                            request.command != "cleanup_call" &&
                            request.command != "middleware_next" &&
                            request.command != "middleware_run")
                       ? Payload(std::move(value))
                       : Payload(body(value));
    channel->receive(response(
        request, Outcome{Outcome::Kind::success, std::move(payload), {}, {}}));
  }
  ~Peer() { channel->close(); }
};
} // namespace test
