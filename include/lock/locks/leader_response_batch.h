#pragma once
#include "lock/locks/lane_layout.h"
#include "lock/locks/mu_state.h"
#include "lock/locks/spsc_queue.h"
#include "lock/rdma/transport.h"
#include <algorithm>
#include <array>
#include <deque>
#include <span>
#include <stdexcept>
#include <vector>

namespace lock {
// Owned exclusively by a lane's communication worker. Group already queued
// actions by destination, preserving FIFO order on each client QP. A tail
// CQE retires all WRs through its exclusive posted-count token. Reserve
// ample SQ space for the lane-zero READY/STOP control messages.
class LeaderResponseBatch {
public:
    static constexpr size_t drain_limit = 64;
    static constexpr size_t sq_credit = 512;
    LeaderResponseBatch(rdma::Transport& net, NodeId self,
                        const std::vector<NodeId>& clients, size_t lane,
                        size_t lane_count, size_t batch_size)
        : net_(net), self_(self), clients_(clients), lane_(lane),
          lane_count_(lane_count), batch_size_(batch_size) {
        if (!batch_size || batch_size > drain_limit)
            throw std::invalid_argument("invalid leader communication batch");
        for (auto peer : clients_) {
            if (!peer || peer >= peers_.size() || peers_[peer].valid)
                throw std::invalid_argument("invalid leader client list");
            peers_[peer].valid = true;
            peers_[peer].messages.reserve(drain_limit);
        }
    }
    void completed(NodeId peer, WorkId end) {
        if (!end) return; // Individual barrier SENDs use token zero.
        auto& p = client(peer);
        if (p.ends.empty() || p.ends.front() != end || end <= p.completed)
            throw std::runtime_error("invalid leader SEND completion prefix");
        p.ends.pop_front();
        p.completed = end;
    }
    template <size_t Capacity>
    size_t drain(SpscQueue<Action, Capacity>& queue) {
        // Bound staging to one drain batch. If a client runs out of credits,
        // keep the unsent suffix and poll CQ again; never reorder its actions.
        if (!staged_) {
            Action a{};
            while (staged_ < drain_limit && queue.try_pop(a)) {
                if (lock_lane(a.message.lock_id, lane_count_) != lane_)
                    throw std::runtime_error("leader response routed to wrong lane");
                a.message.sender = self_;
                client(a.destination).messages.push_back(a.message);
                ++staged_;
            }
        }
        if (!staged_) return 0;
        size_t sent = 0;
        for (NodeId peer : clients_) {
            auto& p = peers_[peer];
            while (p.cursor < p.messages.size()) {
                const size_t room = sq_credit - (p.posted - p.completed);
                const size_t count = std::min({room, batch_size_,
                                               p.messages.size() - p.cursor});
                if (!count) break;
                const WorkId end = p.posted + count;
                net_.send_batch(peer, {p.messages.data() + p.cursor, count}, end, lane_);
                p.ends.push_back(end);
                p.posted = end;
                p.cursor += count;
                sent += count;
                staged_ -= count;
            }
            if (p.cursor == p.messages.size()) {
                p.messages.clear();
                p.cursor = 0;
            }
        }
        return sent;
    }
private:
    struct PeerState {
        bool valid = false;
        std::vector<Message> messages;
        size_t cursor = 0;
        WorkId posted = 0, completed = 0;
        std::deque<WorkId> ends;
    };
    PeerState& client(NodeId peer) {
        if (peer >= peers_.size() || !peers_[peer].valid)
            throw std::runtime_error("response or completion for unknown client");
        return peers_[peer];
    }
    rdma::Transport& net_;
    NodeId self_;
    const std::vector<NodeId>& clients_;
    size_t lane_, lane_count_, batch_size_, staged_ = 0;
    std::array<PeerState, 256> peers_{};
};
} // namespace lock
