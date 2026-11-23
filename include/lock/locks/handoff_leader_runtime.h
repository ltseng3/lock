#pragma once
#include "lock/rdma/transport.h"
#include <cstddef>
#include <vector>
namespace lock {
void run_handoff_leader(rdma::Transport& net, NodeId self,
                        const std::vector<NodeId>& replicas,
                        const std::vector<NodeId>& clients,
                        size_t lock_count, size_t prepared_window,
                        size_t inflight_window, bool profile,
                        size_t lane_count = 1);
} // namespace lock
