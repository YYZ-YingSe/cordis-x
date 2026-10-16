#pragma once
#include <cordis/protocols/session/binary.hpp>
#include <cordis/transports/framing.hpp>
namespace cordis::process {
using namespace cordis::session;
using transports::read_frame;
using transports::write_frame;
} // namespace cordis::process
