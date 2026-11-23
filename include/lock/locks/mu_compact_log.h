#pragma once
#include "lock/locks/mu_encoding.h"
#include "lock/locks/replicated_log.h"
#include "lock/rdma/transport.h"
#include <cstddef>
#include <cstdint>
#include <optional>
#include <vector>

namespace lock {
// Mu has its own 8-byte, inline-replicated append-only log. Unlike the shared
// 128-byte handoff log, local mutation bookkeeping is a bounded ring and does
// not scale with the total number of completed operations.
//
// The leader writes to every follower but requires the completion of a fixed
// majority subset. No follower CPU receives or applies log records in the
// failure-free path; the NIC handles RDMA WRITEs.
class MuCompactLog {
public:
    MuCompactLog(rdma::Transport& net, std::vector<NodeId> replicas,
                 size_t window);
    uint64_t append(const Message& mutation);
    void on_write_complete(NodeId peer, WorkId work_id);
    std::optional<Committed> next_committed();
    size_t inflight() const { return next_ - applied_; }
    size_t capacity() const { return capacity_; }
private:
    struct Pending {
        uint64_t slot = 0;
        Message command{};
        uint8_t quorum_mask = 0;
        uint8_t seen_mask = 0;
        uint8_t remaining = 0;
    };
    rdma::Transport& net_;
    std::vector<NodeId> followers_;
    std::vector<Pending> ring_;
    size_t capacity_ = 0;
    size_t quorum_followers_ = 0;
    uint64_t next_ = 0;
    uint64_t applied_ = 0;
};
} // namespace lock
