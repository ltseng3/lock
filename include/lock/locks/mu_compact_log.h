#pragma once
#include "lock/locks/lane_layout.h"
#include "lock/locks/mu_encoding.h"
#include "lock/locks/replicated_log.h"
#include "lock/locks/replication_progress.h"
#include "lock/rdma/transport.h"
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

namespace lock {
// Single-owner, append-only compact log for one lane. Headers are written
// to all followers in contiguous inline batches. Quorum progress and applied
// state are distinct prefixes; only next_committed exposes ordered commands.
class MuCompactLog {
public:
    static constexpr size_t max_batch = 16;
    MuCompactLog(rdma::Transport& net, std::vector<NodeId> replicas,
                 size_t window, size_t capacity_limit = 0,
                 size_t lane = 0, size_t lane_count = 1);
    // The caller gathers only work already available, up to this limit.
    // Zero means backpressure from the commit ring or a follower SQ.
    size_t available() const;
    void append_batch(std::span<const Message> mutations);
    void on_write_complete(NodeId peer, WorkId work_id);
    std::optional<Committed> next_committed();
    size_t inflight() const { return progress_.posted() - applied_; }
    size_t capacity() const { return layout_.capacity(); }
private:
    struct Pending {
        uint64_t slot = 0;
        Message command{};
    };
    rdma::Transport& net_;
    std::vector<NodeId> followers_;
    std::vector<Pending> ring_;
    CompactLaneLayout layout_;
    ReplicationProgress progress_;
    size_t lane_;
    uint64_t applied_ = 0;
};
} // namespace lock
