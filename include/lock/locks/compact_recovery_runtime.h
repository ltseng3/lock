#pragma once
#include "lock/locks/compact_recovery.h"
#include <chrono>
#include <functional>
#include <vector>

namespace lock {
using RecoveryClock = std::chrono::steady_clock;
struct RecoveredPrefix {
    uint64_t local, remote, length, replayed_at_trigger;
    RecoveryClock::time_point begin, deadline, fenced, scanned, sealed,
                              repaired, epoch_committed, replayed;
};
void install_recovery_trigger();
void check_recovery_deadline(RecoveryClock::time_point deadline);
// R3 seals its old prefix and remains passive; only the designated R2
// returns. replay_to is called with a monotonically increasing length.
RecoveredPrefix recover_compact_prefix(
    rdma::Transport& net, const std::vector<NodeId>& replicas, size_t window,
    CompactRecoveryConfig recovery, const CompactLogView& view,
    const std::function<void(uint64_t)>& replay_to, size_t watchdog_seconds);
void print_recovery_result(const char* prefix, NodeId replacement,
    const RecoveredPrefix& p, RecoveryClock::time_point reconciled,
    RecoveryClock::time_point ready);
} // namespace lock
