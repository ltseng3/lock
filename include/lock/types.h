#pragma once
#include <cstdint>
#include <stdexcept>
#include <type_traits>
namespace lock {
using NodeId = uint32_t;
using LockId = uint32_t;
using RequestId = uint64_t;
using WorkId = uint64_t;
constexpr uint32_t kWireVersion = 1;
inline RequestId make_request_id(NodeId node, uint64_t sequence) {
    if (node == 0 || node > 65535 || sequence == 0 || sequence >= (1ULL << 48))
        throw std::invalid_argument("request ID out of range");
    return (static_cast<uint64_t>(node) << 48) | sequence;
}
inline NodeId request_node(RequestId request) { return static_cast<NodeId>(request >> 48); }
inline uint64_t request_sequence(RequestId request) { return request & ((1ULL << 48) - 1); }
enum class Kind : uint32_t {
    Acquire = 1, Release = 2, Grant = 3, ReleaseAck = 4,
    Order = 5, Start = 6, Next = 7, Handoff = 8,
    Ready = 9, Stop = 10, Error = 11, RetirementGc = 12,
    RecoverySeal = 13, RecoveryBegin = 14, RecoveryAcquire = 15,
    RecoveryRelease = 16, RecoveryDone = 17, RecoveryReady = 18
};
struct Message {
    uint64_t request = 0;
    uint64_t other = 0;
    uint64_t position = 0;
    uint64_t deadline_ns = 0;
    uint64_t value = 0;
    uint32_t kind = 0;
    uint32_t lock_id = 0;
    uint32_t sender = 0;
    uint32_t flags = 0;
    uint32_t lane = 0;
    uint32_t version = kWireVersion;
};
static_assert(sizeof(Message) == 64);
static_assert(std::is_trivially_copyable_v<Message>);
inline Message message(Kind kind, NodeId sender, LockId lock_id = 0, RequestId request = 0) {
    Message m;
    m.kind = static_cast<uint32_t>(kind);
    m.sender = sender;
    m.lock_id = lock_id;
    m.request = request;
    return m;
}
inline Kind message_kind(const Message& m) {
    if (m.version != kWireVersion ||
        m.kind < static_cast<uint32_t>(Kind::Acquire) ||
        m.kind > static_cast<uint32_t>(Kind::RecoveryReady))
        throw std::runtime_error("invalid wire message");
    return static_cast<Kind>(m.kind);
}
} // namespace lock
