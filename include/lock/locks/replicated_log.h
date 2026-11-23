#pragma once
#include "lock/rdma/transport.h"
#include <cstddef>
#include <cstdint>
#include <optional>
#include <unordered_map>
#include <vector>
namespace lock {
// One replicated record per mutation; never reused in the initial implementation.
struct alignas(64) LogRecord {
    Message command{};
    uint64_t retired = 0;  // Optional one-sided release evidence for handoff.
    uint8_t reserved[56]{};
};
static_assert(sizeof(LogRecord) == 128);

struct Committed {
    uint64_t global_slot;
    Message command;
};
class ReplicatedLog {
public:
    ReplicatedLog(rdma::Transport& transport, std::vector<NodeId> replicas,
                  bool quorum_only_signaling = false);
    uint64_t append(const Message& command);
    void on_write_complete(WorkId work_id);
    std::optional<Committed> next_committed();
    size_t inflight() const { return pending_.size() - applied_; }
    size_t capacity() const { return capacity_; }
private:
    struct Pending {
        uint32_t acknowledgments = 1; // Local replica.
        Message command{};
    };
    rdma::Transport& net_;
    std::vector<NodeId> replicas_;
    bool quorum_only_signaling_ = false;
    std::vector<Pending> pending_;
    std::unordered_map<WorkId, uint64_t> writes_;
    uint64_t next_work_ = 1;
    uint64_t applied_ = 0;
    size_t capacity_ = 0;
};
} // namespace lock
