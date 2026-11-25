#pragma once
#include "lock/locks/lane_layout.h"
#include "lock/locks/retirement_gc.h"
#include "lock/locks/mu_state.h"
#include "lock/locks/spsc_queue.h"
#include "lock/rdma/transport.h"
#include <algorithm>
#include <array>
#include "lock/rdma/completion_prefix.h"
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
                        size_t lane_count, size_t batch_size, bool gc = false)
        : net_(net), self_(self), lane_(lane), lane_count_(lane_count),
          batch_size_(batch_size), gc_(gc), peers_(clients.size()) {
        if (!batch_size || batch_size > drain_limit)
            throw std::invalid_argument("invalid leader communication batch");
        index_.fill(-1);
        for (size_t i = 0; i < clients.size(); ++i) {
            const auto peer = clients[i];
            if (!peer || peer >= index_.size() || index_[peer] != -1)
                throw std::invalid_argument("invalid leader client list");
            index_[peer] = static_cast<int>(i);
            peers_[i].peer = peer;
            peers_[i].messages.reserve(drain_limit);
        }
    }
    void completed(NodeId peer, WorkId end) {
        if (!end) return; // Individual barrier SENDs use token zero.
        auto& p = client(peer);
        if (!p.isolated) p.progress.complete(end);
    }
    // Called by the communication worker after transport isolation. Abandon
    // only this destination's staged suffix and credits; do not pretend its
    // outstanding SENDs completed. Isolation is irreversible for this run.
    void isolate(NodeId peer) {
        auto& p = client(peer);
        if (p.isolated) return;
        p.isolated = true;
        staged_ -= p.messages.size() - p.cursor;
        p.messages.clear();
        p.cursor = 0;
        p.gc_count = 0;
    }
    template <size_t Capacity>
    size_t drain(SpscQueue<Action, Capacity>& queue) {
        // Bound staging to one drain batch. If a client runs out of credits,
        // keep the unsent suffix and poll CQ again; never reorder its actions.
        if (!staged_) {
            Action a{};
            size_t consumed = 0;
            while (consumed < drain_limit && queue.try_pop(a)) {
                ++consumed;
                if (lock_lane(a.message.lock_id, lane_count_) != lane_)
                    throw std::runtime_error("leader response routed to wrong lane");
                a.message.sender = self_;
                auto& p = client(a.destination);
                if (p.isolated) continue;
                if (gc_ && message_kind(a.message) == Kind::ReleaseAck) {
                    if (request_node(a.message.request) != a.destination)
                        throw std::runtime_error("GC action destination mismatch");
                    p.gc_ids[p.gc_count++] = a.message.request;
                    if (p.gc_count == p.gc_ids.size()) flush_gc(p);
                } else {
                    p.messages.push_back(a.message);
                    ++staged_;
                }
            }
            // Flush even singletons. A GC packet never overtakes an earlier
            // NEXT on its QP and only contains no-successor retirements.
            if (gc_) for (auto& p : peers_) flush_gc(p);
        }
        if (!staged_) return 0;
        size_t sent = 0;
        for (auto& p : peers_) {
            if (p.isolated) continue;
            while (p.cursor < p.messages.size()) {
                const size_t room = sq_credit -
                    (p.progress.posted() - p.progress.completed());
                const size_t count = std::min({room, batch_size_,
                                               p.messages.size() - p.cursor});
                if (!count) break;
                const WorkId end = p.progress.posted() + count;
                net_.send_batch(p.peer, {p.messages.data() + p.cursor, count}, end, lane_);
                p.progress.post(end);
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
        NodeId peer = 0;
        bool isolated = false;
        std::vector<Message> messages;
        size_t cursor = 0;
        rdma::CompletionPrefix<sq_credit> progress;
        GcIds gc_ids{};
        size_t gc_count = 0;
    };
    void flush_gc(PeerState& p) {
        if (!p.gc_count) return;
        p.messages.push_back(retirement_gc_message(
            self_, lane_, {p.gc_ids.data(), p.gc_count}));
        p.gc_count = 0;
        ++staged_;
    }
    PeerState& client(NodeId peer) {
        if (peer >= index_.size() || index_[peer] == -1)
            throw std::runtime_error("response or completion for unknown client");
        return peers_[static_cast<size_t>(index_[peer])];
    }
    rdma::Transport& net_;
    NodeId self_;
    size_t lane_, lane_count_, batch_size_, staged_ = 0;
    bool gc_;
    std::array<int, 256> index_;
    std::vector<PeerState> peers_;
};
} // namespace lock
