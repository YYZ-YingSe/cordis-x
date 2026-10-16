#pragma once
#include <algorithm>
#include <array>
#include <cordis/protocols/session/binary.hpp>
#include <cordis/runtime/native.hpp>
#include <deque>
#include <optional>
#include <unistd.h>

namespace cordis::transports {
// One serialized, bounded output queue. Pipe backpressure suspends Asio I/O;
// it cannot block the coroutine executor or hold a business/cleanup lane.
class Stdio {
  asio::posix::stream_descriptor input_, output_;
  std::array<uint8_t, 64 * 1024> input_buffer_{};
  std::size_t input_begin_ = 0, input_end_ = 0, buffered_frames_ = 0;
  std::deque<std::shared_ptr<std::vector<uint8_t>>> writes_;
  std::size_t bytes_ = 0;
  bool failed_ = false;
  std::function<void()> failure_;
  static int duplicate(int fd) {
    int result = ::dup(fd);
    if (result < 0)
      throw std::runtime_error("cannot duplicate stdio descriptor");
    return result;
  }
  Async<std::size_t> read_exact(uint8_t *destination, std::size_t size) {
    std::size_t copied = 0;
    while (copied != size) {
      if (input_begin_ == input_end_) {
        buffered_frames_ = 0;
        auto [error, count] = co_await input_.async_read_some(
            asio::buffer(input_buffer_), corral::asio_nothrow_awaitable);
        if (error == asio::error::eof)
          co_return copied;
        if (error)
          throw std::system_error(error);
        input_begin_ = 0;
        input_end_ = count;
      }
      const auto count = std::min(size - copied, input_end_ - input_begin_);
      std::copy_n(input_buffer_.data() + input_begin_, count,
                  destination + copied);
      input_begin_ += count;
      copied += count;
    }
    co_return copied;
  }
  void write_next() {
    auto bytes = writes_.front();
    asio::async_write(output_, asio::buffer(*bytes),
                      [this, bytes](std::error_code error, std::size_t) {
                        if (failed_)
                          return;
                        if (error) {
                          abort();
                          failure_();
                          return;
                        }
                        bytes_ -= bytes->size();
                        writes_.pop_front();
                        if (!writes_.empty())
                          write_next();
                      });
  }

public:
  Stdio(native::Runtime &runtime, std::function<void()> failure)
      : input_(runtime.io(), duplicate(STDIN_FILENO)),
        output_(runtime.io(), duplicate(STDOUT_FILENO)),
        failure_(std::move(failure)) {}
  void stop_reading() {
    std::error_code ignored;
    input_.close(ignored);
  }
  void abort() {
    failed_ = true;
    stop_reading();
    std::error_code ignored;
    output_.close(ignored);
    writes_.clear();
    bytes_ = 0;
  }
  void write(std::vector<uint8_t> payload) {
    if (failed_)
      throw std::runtime_error("stdio closed");
    const auto length = static_cast<uint32_t>(payload.size());
    auto bytes = std::make_shared<std::vector<uint8_t>>();
    bytes->reserve(payload.size() + 4);
    for (int shift : {24, 16, 8, 0})
      bytes->push_back(static_cast<uint8_t>(length >> shift));
    bytes->insert(bytes->end(), payload.begin(), payload.end());
    if (writes_.size() >= 256 ||
        bytes->size() > 16 * session::max_message - bytes_) {
      abort();
      failure_();
      throw std::runtime_error("stdio output capacity exhausted");
    }
    bytes_ += bytes->size();
    writes_.push_back(std::move(bytes));
    if (writes_.size() == 1)
      write_next();
  }
  Async<std::optional<session::Frame>> read(Validation validation = Validation::Checked) {
    // Buffered frames need no extra reactor turn for their bodies. Yield after
    // a bounded batch so queued business, completions and cleanup can progress.
    if (buffered_frames_ == 16) {
      buffered_frames_ = 0;
      co_await native::completed<bool>([](auto done) { done(true); });
    }
    if (!input_.is_open())
      co_return std::nullopt;
    std::array<uint8_t, 4> header{};
    const auto count = co_await read_exact(header.data(), header.size());
    if (count == 0)
      co_return std::nullopt;
    if (count != header.size())
      throw std::runtime_error("truncated frame header");
    uint32_t length = 0;
    for (auto byte : header)
      length = (length << 8) | byte;
    if (length < 1 || length > session::max_message)
      throw std::runtime_error("invalid frame length");
    std::vector<uint8_t> bytes(length);
    if (co_await read_exact(bytes.data(), bytes.size()) != bytes.size())
      throw std::runtime_error("truncated frame body");
    auto value = session::decode(bytes, validation);
    ++buffered_frames_;
    co_return value;
  }
};
} // namespace cordis::transports
