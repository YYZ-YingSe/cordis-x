#pragma once
#include <algorithm>
#include <array>
#include <cordis/protocols/session/binary.hpp>
#include <cordis/runtime/native.hpp>
#include <functional>
#include <map>
#include <sstream>

namespace cordis::http {
using namespace cordis::session;
class Server {
  using Socket = asio::ip::tcp::socket;
  native::Runtime &runtime;
  std::function<Async<Response>(Request)> dispatch;
  std::function<Validation()> validation;
  asio::ip::tcp::acceptor listener;
  std::map<std::uint64_t, std::weak_ptr<Socket>> connections;
  std::uint64_t next_connection = 0;
  std::string url;
  static bool resource_id(const std::string &id) {
    return id.size() >= 2 && id.size() <= 21 && id[0] == 'r' &&
           std::all_of(id.begin() + 1, id.end(),
                       [](char c) { return c >= '0' && c <= '9'; });
  }
  Async<void> http(std::shared_ptr<Socket> socket) {
    asio::steady_timer deadline(runtime.io());
    auto arm_deadline = [&] {
      deadline.expires_after(std::chrono::seconds(5));
      deadline.async_wait([socket](std::error_code error) {
        if (!error) {
          std::error_code ignored;
          socket->close(ignored);
        }
      });
    };
    std::string bytes;
    char buffer[4096];
    for (;;) {
      arm_deadline();
      std::size_t split;
      while ((split = bytes.find("\r\n\r\n")) == std::string::npos) {
        const auto n = co_await socket->async_read_some(asio::buffer(buffer),
                                                        corral::asio_awaitable);
        if (n == 0)
          co_return;
        bytes.append(buffer, static_cast<std::size_t>(n));
        const auto end = bytes.find("\r\n\r\n");
        if ((end == std::string::npos ? bytes.size() : end + 4) > 16 * 1024)
          throw std::runtime_error("HTTP headers too large");
      }
      std::istringstream header(bytes.substr(0, split));
      std::string line;
      std::getline(header, line);
      std::istringstream first(line);
      std::string method, path, version;
      first >> method >> path >> version;
      if (version != "HTTP/1.1")
        throw std::runtime_error("unsupported HTTP version");
      if (method != "POST" || path != "/cordis/call")
        throw std::runtime_error("unsupported HTTP request");
      std::map<std::string, std::string> headers;
      while (std::getline(header, line)) {
        if (!line.empty() && line.back() == '\r')
          line.pop_back();
        const auto colon = line.find(':');
        if (colon == std::string::npos)
          throw std::runtime_error("invalid HTTP header");
        auto key = line.substr(0, colon);
        std::transform(
            key.begin(), key.end(), key.begin(),
            [](unsigned char c) { return static_cast<char>(std::tolower(c)); });
        auto value = line.substr(colon + 1);
        value.erase(0, value.find_first_not_of(" \t"));
        if (!headers.emplace(key, value).second)
          throw std::runtime_error("duplicate HTTP header");
      }
      if (headers.count("transfer-encoding"))
        throw std::runtime_error(
            "chunked requests not supported by this profile");
      const auto &length_text = headers.at("content-length");
      if (length_text.empty() || length_text.size() > 7 ||
          !std::all_of(length_text.begin(), length_text.end(),
                       [](char c) { return c >= '0' && c <= '9'; }))
        throw std::runtime_error("invalid Content-Length");
      const auto length = static_cast<std::size_t>(std::stoul(length_text));
      if (length > max_message)
        throw std::runtime_error("HTTP body too large");
      const auto type = headers.at("content-type");
      if (type != content_type)
        throw std::runtime_error("unsupported HTTP content type");
      bytes.erase(0, split + 4);
      while (bytes.size() < length) {
        const auto n = co_await socket->async_read_some(
            asio::buffer(buffer,
                         std::min(sizeof(buffer), length - bytes.size())),
            corral::asio_awaitable);
        if (n == 0)
          co_return;
        bytes.append(buffer, static_cast<std::size_t>(n));
      }
      auto frame = decode(reinterpret_cast<const uint8_t *>(bytes.data()),
                          length, validation());
      bytes.erase(0, length); // Retain a pipelined next request, if present.
      bool close = false;
      if (auto found = headers.find("connection"); found != headers.end()) {
        std::string token;
        std::istringstream tokens(found->second);
        while (std::getline(tokens, token, ',')) {
          token.erase(0, token.find_first_not_of(" \t"));
          token.erase(token.find_last_not_of(" \t") + 1);
          std::transform(token.begin(), token.end(), token.begin(),
                         [](unsigned char c) {
                           return static_cast<char>(std::tolower(c));
                         });
          close = close || token == "close";
        }
      }
      if (!std::holds_alternative<Request>(frame))
        throw std::runtime_error("expected HTTP request");
      auto request = std::get<Request>(std::move(frame));
      if (request.command != "call" && request.command != "resource_acquire" &&
          request.command != "resource_call" &&
          request.command != "resource_release")
        throw std::runtime_error("command requires session endpoint");
      // The transport's read/write timeout does not shorten the host's business
      // deadline. A suspended call can legitimately outlive the I/O timeout.
      deadline.cancel();
      const auto response = co_await dispatch(std::move(request));
      const bool ok = response.outcome.kind == Outcome::Kind::success;
      int status = 200;
      if (!ok) {
        const auto code = response.outcome.code;
        status = code == "unknown_method"                      ? 404
                 : code == "overloaded"                        ? 429
                 : code == "internal"                          ? 500
                 : code == "stale_session" || code == "closed" ? 409
                                                               : 400;
      }
      const auto body = encode(Frame(response));
      const auto prefix =
          "HTTP/1.1 " + std::to_string(status) +
          (ok ? " OK\r\n" : " Error\r\n") + "Content-Type: " + content_type +
          "\r\nContent-Length: " + std::to_string(body.size()) +
          (close ? "\r\nConnection: close\r\n\r\n" : "\r\n\r\n");
      arm_deadline();
      const std::array<asio::const_buffer, 2> buffers{asio::buffer(prefix),
                                                      asio::buffer(body)};
      co_await asio::async_write(*socket, buffers, corral::asio_awaitable);
      deadline.cancel();
      if (close)
        co_return;
    }
  }

public:
  void start() {
    listener.open(asio::ip::tcp::v4());
    listener.bind({asio::ip::address_v4::loopback(), 0});
    listener.listen(16);
    url =
        "http://127.0.0.1:" + std::to_string(listener.local_endpoint().port()) +
        "/cordis/call";
    runtime.submit([this]() -> Async<void> {
      for (;;) {
        auto socket = std::make_shared<Socket>(runtime.io());
        try {
          co_await listener.async_accept(*socket, corral::asio_awaitable);
        } catch (const std::system_error &) {
          co_return;
        }
        if (connections.size() >= 64)
          continue;
        const auto id = ++next_connection;
        connections.emplace(id, socket);
        runtime.submit([this, socket, id]() -> Async<void> {
          struct Remove {
            Server &server;
            std::uint64_t id;
            ~Remove() { server.connections.erase(id); }
          } remove{*this, id};
          try {
            socket->set_option(asio::ip::tcp::no_delay(true));
            co_await http(socket);
          } catch (const std::exception
                       &) { /* A failed transport carries no known outcome. */
          }
        });
      }
    });
  }
  void stop() {
    runtime.post([this] {
      std::error_code ignored;
      listener.close(ignored);
      for (auto &[_, weak] : connections)
        if (auto socket = weak.lock())
          socket->close(ignored);
    });
  }
  Server(native::Runtime &runtime,
         std::function<Async<Response>(Request)> dispatch,
         std::function<Validation()> validation)
      : runtime(runtime), dispatch(std::move(dispatch)),
        validation(std::move(validation)), listener(runtime.io()) {}
  // The owner stops this server and drains Runtime before destroying either.
  const std::string &endpoint() const { return url; }
};
} // namespace cordis::http
