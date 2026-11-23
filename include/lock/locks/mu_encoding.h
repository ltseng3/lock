#pragma once
#include "lock/types.h"
#include <cstdint>
#include <limits>
#include <stdexcept>

namespace lock {
// Mu-only, eight-byte log format. Both acquire and release are recoverable
// from the log slot plus this word. The leader retains the full Message while
// the common path runs. No truncation is permitted.
//
//  [63]    release flag
//  [62:55] client node ID (1..255)
//  [54:39] lock ID (0..65535)
//  [38:0]  request sequence (1..2^39-1)
inline constexpr uint64_t kMuSequenceMask = (1ULL << 39) - 1;
inline constexpr uint32_t kMuMaxLockId = 65535;
inline constexpr NodeId kMuMaxClientNode = 255;

inline uint64_t pack_mu_command(const Message& msg) {
    const Kind kind = message_kind(msg);
    if (kind != Kind::Acquire && kind != Kind::Release)
        throw std::invalid_argument("Mu log requires acquire or release");
    const NodeId node = request_node(msg.request);
    const uint64_t seq = request_sequence(msg.request);
    if (node == 0 || node > kMuMaxClientNode || node != msg.sender ||
        msg.lock_id > kMuMaxLockId || seq == 0 || seq > kMuSequenceMask)
        throw std::invalid_argument("Mu log entry cannot be represented in 8 bytes");
    return (static_cast<uint64_t>(kind == Kind::Release) << 63) |
           (static_cast<uint64_t>(node) << 55) |
           (static_cast<uint64_t>(msg.lock_id) << 39) | seq;
}

inline Message unpack_mu_command(uint64_t word) {
    const auto node = static_cast<NodeId>((word >> 55) & 0xff);
    const auto lock = static_cast<LockId>((word >> 39) & 0xffff);
    const uint64_t seq = word & kMuSequenceMask;
    if (node == 0 || seq == 0)
        throw std::invalid_argument("invalid packed Mu log entry");
    return message((word >> 63) ? Kind::Release : Kind::Acquire,
                   node, lock, make_request_id(node, seq));
}
static_assert(sizeof(uint64_t) == 8);
} // namespace lock
