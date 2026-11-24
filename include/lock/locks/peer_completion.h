#pragma once
#include "lock/types.h"
#include "lock/rdma/transport.h"
#include <cstddef>
#include <stdexcept>
#include <unordered_map>

namespace lock {
// Client-only memory: the receive rings end before kLogOffset, and
// scratch allocations start in the last 16 MiB. Clients have no log here.
// Slots are never reset on submission: a collision can only cause a miss.
// Request IDs are unique within a connected benchmark session.
constexpr size_t kPeerCompletionSlots = 1u << 20;
constexpr size_t kPeerCompletionBytes = kPeerCompletionSlots * sizeof(uint64_t);
static_assert(rdma::kLogOffset + kPeerCompletionBytes < rdma::kScratchOffset);
inline size_t peer_completion_offset(RequestId request) {
    if (!request_node(request) || !request_sequence(request))
        throw std::invalid_argument("invalid peer completion request");
    return rdma::kLogOffset +
           (request_sequence(request) % kPeerCompletionSlots) * sizeof(uint64_t);
}
inline bool peer_completion_matches(RequestId expected, uint64_t observed) {
    return expected != 0 && observed == expected;
}

// A READ winner still expects exactly one normal HANDOFF. Retain the
// identity even after the operation slot is recycled. No other message
// can consume this receipt; a missing/incorrect proof fails closed.
class PeerReadReceipts {
    struct Proof { LockId lock; RequestId predecessor; uint64_t position; };
    std::unordered_map<RequestId, Proof> pending_;
public:
    static constexpr size_t limit = 4096;
    size_t size() const { return pending_.size(); }
    bool empty() const { return pending_.empty(); }
    void expect(RequestId request, LockId lock, RequestId predecessor,
                uint64_t position) {
        if (!request || !predecessor || request == predecessor ||
            pending_.size() >= limit ||
            !pending_.emplace(request, Proof{lock, predecessor, position}).second)
            throw std::runtime_error("invalid peer READ receipt registration");
    }
    bool consume(NodeId sender, const Message& m) {
        auto it = pending_.find(m.request);
        if (it == pending_.end()) return false;
        const auto& p = it->second;
        if (message_kind(m) != Kind::Handoff || m.sender != sender ||
            m.lock_id != p.lock || m.other != p.predecessor ||
            m.position != p.position || sender != request_node(p.predecessor))
            throw std::runtime_error("invalid redundant HANDOFF proof");
        pending_.erase(it);
        return true;
    }
};
} // namespace lock
