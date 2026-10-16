#pragma once
#include <cordis/protocols/session/binary.hpp>
#include <istream>
#include <ostream>
namespace cordis::transports {
inline bool read_frame(std::istream &input, session::Frame &frame,
                       Validation validation = Validation::Checked) {
  unsigned char header[4];
  input.read(reinterpret_cast<char *>(header), 4);
  if (input.gcount() == 0 && input.eof())
    return false;
  if (input.gcount() != 4)
    throw std::runtime_error("truncated frame length");
  uint32_t length = 0;
  for (auto byte : header)
    length = (length << 8) | byte;
  if (length < 1 || length > session::max_message)
    throw std::runtime_error("invalid frame length");
  std::vector<uint8_t> bytes(length);
  input.read(reinterpret_cast<char *>(bytes.data()), length);
  if (!input)
    throw std::runtime_error("truncated frame body");
  frame = session::decode(bytes, validation);
  return true;
}
inline void write_frame(std::ostream &output,
                        const std::vector<uint8_t> &bytes) {
  if (bytes.empty() || bytes.size() > session::max_message)
    throw std::runtime_error("invalid frame length");
  auto length = static_cast<uint32_t>(bytes.size());
  unsigned char header[] = {static_cast<unsigned char>(length >> 24),
                            static_cast<unsigned char>(length >> 16),
                            static_cast<unsigned char>(length >> 8),
                            static_cast<unsigned char>(length)};
  output.write(reinterpret_cast<const char *>(header), 4);
  output.write(reinterpret_cast<const char *>(bytes.data()), bytes.size());
  output.flush();
  if (!output)
    throw std::runtime_error("frame write failed");
}
} // namespace cordis::transports
