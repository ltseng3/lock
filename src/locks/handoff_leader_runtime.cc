#include "lock/locks/handoff_leader_runtime.h"
#include "lock/locks/handoff_layout.h"
#include "lock/locks/handoff_state.h"
#include "lock/locks/mu_compact_log.h"
#include "lock/locks/mu_encoding.h"
#include "lock/locks/spsc_queue.h"
#include "lock/locks/lane_layout.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <set>
#include <stdexcept>
#include <thread>
#include <vector>

namespace lock {
namespace {
using Clock = std::chrono::steady_clock;
std::mutex profile_output_mutex;
struct Shared {
    SpscQueue<Message> requests;
    SpscQueue<Action> responses;
    std::atomic<uint64_t> acquire_rx{0}, responses_sent{0}, send_cqe{0};
    std::atomic<bool> stopped{false}, failed{false};
    std::exception_ptr failure;
};
struct JoinWorkers {
    std::vector<std::unique_ptr<Shared>>& lanes;
    std::vector<std::thread>& workers;
    ~JoinWorkers() {
        for (auto& lane : lanes)
            lane->stopped.store(true, std::memory_order_release);
        for (auto& worker : workers)
            if (worker.joinable()) worker.join();
    }
};
void submit_action(Shared& s, const Action& action) {
    if (!s.responses.try_push(action))
        throw std::runtime_error("handoff response queue capacity exceeded");
}
void replication_loop(Shared& s, rdma::Transport& net,
                      const std::vector<NodeId>& replicas,
                      size_t lock_count, size_t prepared_window,
                      size_t window, bool profile,
                      size_t lane, size_t lane_count) {
    try {
        // Half the registered log area holds 8-byte replicated headers. The
        // adjacent half holds one 8-byte marker per slot, written by the
        // completed client to the leader's NIC-exposed memory.
        const size_t capacity = handoff_log_capacity(net.memory_bytes());
        MuCompactLog log(net, replicas, window, capacity, lane, lane_count);
        HandoffState state(lock_count, prepared_window);
        std::deque<Message> append_pending;
        std::vector<rdma::Completion> cqes;
        cqes.reserve(64);
        auto profile_start = Clock::now();
        uint64_t loops = 0, commits = 0, write_cqe = 0, retire_reads = 0;
        uint64_t emitted = 0;
        size_t max_inflight = 0, max_pending = 0;
        // The sidecar marker region is contiguous; compute its base once
        // rather than checking offset arithmetic for every DMA-visible load.
        auto* marker_base = static_cast<uint64_t*>(net.at(
            handoff_marker_offset(net.memory_bytes(), 0),
            capacity * sizeof(uint64_t)));
        const std::function<uint64_t(uint64_t)> read_marker =
            [&](uint64_t slot) {
                ++retire_reads;
                if (slot >= capacity)
                    throw std::out_of_range("handoff marker slot out of range");
                return std::atomic_ref<uint64_t>(marker_base[slot])
                    .load(std::memory_order_acquire);
            };
        auto publish_retirement = [&](HandoffStep&& step) {
            for (const auto& action : step.actions) {
                submit_action(s, action);
                ++emitted;
            }
            for (const auto& cmd : step.append)
                append_pending.push_back(cmd);
        };
        // Never spin across the entire active-lock set on every empty-CQ
        // iteration. Poll periodically for releases with no new commits,
        // and check only the relevant lock before assigning a successor.
        constexpr auto kRetirementScanPeriod = std::chrono::microseconds(32);
        auto next_retirement_scan = Clock::now() + kRetirementScanPeriod;
        while (!s.stopped.load(std::memory_order_acquire)) {
            net.poll_replication_into(cqes, 64, lane);
            for (const auto& c : cqes) {
                if (c.kind != rdma::CompletionKind::Write)
                    throw std::runtime_error("unexpected handoff replication CQE");
                log.on_write_complete(c.peer, c.work_id);
                ++write_cqe;
            }

            // Markers are written one-sidedly by clients and do not generate
            // a leader CQE. Periodic scans guarantee eventual retirement even
            // if no further acquisition or replication completion arrives.
            const auto now = Clock::now();
            if (now >= next_retirement_scan) {
                publish_retirement(state.retire(read_marker));
                next_retirement_scan = now + kRetirementScanPeriod;
            }

            // Preserve the retirement-first optimization without repeatedly
            // scanning unrelated active locks. This targeted check is done
            // before each new commit gets assigned its immediate predecessor.
            while (auto committed = log.next_committed()) {
                publish_retirement(state.retire_lock(
                    committed->command.lock_id, read_marker));
                ++commits;
                for (const auto& action : state.committed(*committed)) {
                    submit_action(s, action);
                    ++emitted;
                }
            }

            Message request{};
            // This thread exclusively owns the per-lock queue and reservation
            // state. Client receives never mutate those structures directly.
            size_t admitted = 0;
            while (admitted++ < 64 && s.requests.try_pop(request)) {
                if (lock_lane(request.lock_id, lane_count) != lane)
                    throw std::runtime_error("handoff request routed to wrong lane");
                auto step = state.admit(request);
                for (const auto& cmd : step.append)
                    append_pending.push_back(cmd);
            }

            max_pending = std::max(max_pending, append_pending.size() +
                                                 s.requests.size());
            while (!append_pending.empty() && log.inflight() < window) {
                log.append(append_pending.front());
                append_pending.pop_front();
            }
            max_inflight = std::max(max_inflight, log.inflight());

            if (profile && ((++loops & 4095ULL) == 0)) {
                const auto now = Clock::now();
                const double elapsed =
                    std::chrono::duration<double>(now - profile_start).count();
                if (elapsed >= 1.0) {
                    const auto received = s.acquire_rx.exchange(0);
                    const auto sent = s.responses_sent.exchange(0);
                    const auto sends = s.send_cqe.exchange(0);
                    if (received || commits) {
                        std::lock_guard<std::mutex> output_lock(profile_output_mutex);
                        std::cout << "HANDOFF_PROFILE"
                                  << " lane=" << lane
                                  << " seconds=" << elapsed
                                  << " acquire_rx=" << received
                                  << " commits=" << commits
                                  << " replication_write_cqe=" << write_cqe
                                  << " send_cqe=" << sends
                                  << " responses_sent=" << sent
                                  << " actions_enqueued=" << emitted
                                  << " marker_reads=" << retire_reads
                                  << " max_inflight=" << max_inflight
                                  << " window=" << window
                                  << " max_pending=" << max_pending
                                  << std::endl;
                    }
                    profile_start = now;
                    commits = write_cqe = retire_reads = emitted = 0;
                    max_inflight = max_pending = 0;
                }
            }
        }
    } catch (...) {
        s.failure = std::current_exception();
        s.failed.store(true, std::memory_order_release);
    }
}
} // namespace

void run_handoff_leader(rdma::Transport& net, NodeId self,
                        const std::vector<NodeId>& replicas,
                        const std::vector<NodeId>& clients,
                        size_t lock_count, size_t prepared_window,
                        size_t inflight_window, bool profile,
                        size_t lane_count) {
    if (lane_count < 1 || lane_count > 8)
        throw std::invalid_argument("invalid handoff lane count");
    std::vector<std::unique_ptr<Shared>> lanes;
    std::vector<std::thread> workers;
    lanes.reserve(lane_count);
    workers.reserve(lane_count);
    for (size_t lane = 0; lane < lane_count; ++lane)
        lanes.push_back(std::make_unique<Shared>());
    JoinWorkers join{lanes, workers};
    for (size_t lane = 0; lane < lane_count; ++lane) {
        Shared* shard = lanes[lane].get();
        workers.emplace_back([&, shard, lane] {
            replication_loop(*shard, net, replicas, lock_count, prepared_window,
                             inflight_window, profile, lane, lane_count);
        });
    }

    // The communication thread exclusively owns client RC QPs and their CQ.
    // The worker owns follower QPs and the separate replication CQ.
    std::vector<rdma::Completion> cqes;
    cqes.reserve(64);
    std::set<NodeId> ready, finished;
    bool started = false;
    while (true) {
        for (const auto& shard : lanes)
            if (shard->failed.load(std::memory_order_acquire))
                std::rethrow_exception(shard->failure);
        net.poll_into(cqes);
        for (const auto& c : cqes) {
            if (c.kind == rdma::CompletionKind::Send) {
                if (profile) lanes[0]->send_cqe.fetch_add(1, std::memory_order_relaxed);
                continue;
            }
            if (c.kind != rdma::CompletionKind::Receive)
                throw std::runtime_error("unexpected handoff communication CQE");
            const auto& m = c.message;
            if (m.sender != c.peer)
                throw std::runtime_error("handoff sender mismatch");
            const Kind kind = message_kind(m);
            if (kind == Kind::Ready && !started) {
                if (std::find(clients.begin(), clients.end(), c.peer) == clients.end() ||
                    !ready.insert(c.peer).second)
                    throw std::runtime_error("invalid handoff readiness");
                if (ready.size() == clients.size()) {
                    started = true;
                    for (NodeId id : clients)
                        net.send(id, message(Kind::Ready, self));
                }
            } else if (kind == Kind::Stop && started) {
                if (std::find(clients.begin(), clients.end(), c.peer) == clients.end() ||
                    !finished.insert(c.peer).second)
                    throw std::runtime_error("invalid handoff shutdown");
                if (finished.size() == clients.size())
                    for (NodeId id : clients)
                        net.send(id, message(Kind::Stop, self));
            } else if (kind == Kind::Acquire && started) {
                if (std::find(clients.begin(), clients.end(), c.peer) == clients.end() ||
                    request_node(m.request) != c.peer || m.lock_id >= lock_count)
                    throw std::runtime_error("invalid handoff request");
                // Reject unrepresentable identifiers before assigning log slots.
                (void)pack_mu_command(m);
                const size_t lane = lock_lane(m.lock_id, lane_count);
                auto& shard = *lanes[lane];
                if (!shard.requests.try_push(m))
                    throw std::runtime_error("handoff request queue capacity exceeded");
                if (profile)
                    shard.acquire_rx.fetch_add(1, std::memory_order_relaxed);
            } else {
                throw std::runtime_error("unexpected handoff client message");
            }
        }

        Action action{};
        for (auto& shard : lanes) {
            // A busy hot-lock lane must not starve other lanes on the shared
            // client-communication thread. Drain a bounded fair batch.
            size_t sent = 0;
            while (sent++ < 64 && shard->responses.try_pop(action)) {
                action.message.sender = self;
                // Client SENDs share the communication thread's QPs.
                net.send(action.destination, action.message, 0, false);
                if (profile)
                    shard->responses_sent.fetch_add(1, std::memory_order_relaxed);
            }
        }
    }
}
} // namespace lock
