#pragma once
#include "lock/rdma/transport.h"
#include "lock/locks/mu_recovery.h"
#include <cstddef>
#include <vector>

namespace lock {
struct MuPromotion {
    MuState state;
    std::vector<Message> pending;
    std::vector<Action> responses;
};

void run_mu_standby(rdma::Transport& net, const std::vector<NodeId>& replicas,
                   const std::vector<NodeId>& clients, size_t locks, size_t window,
                   bool profile, size_t replication_batch, size_t communication_batch,
                   CompactRecoveryConfig recovery, size_t watchdog_seconds);
void run_mu_leader(rdma::Transport& net, NodeId self,
                   const std::vector<NodeId>& replicas,
                   const std::vector<NodeId>& clients, size_t lock_count,
                   size_t inflight_window, bool profile,
                   size_t lane_count = 1, size_t replication_batch = 8,
                        size_t communication_batch = 64,
                        CompactRecoveryConfig recovery = {}, MuPromotion* promotion = nullptr);
} // namespace lock
