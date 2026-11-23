#include "lock/locks/mu_leader_runtime.h"
#include "lock/locks/mu_compact_log.h"
#include "lock/locks/mu_state.h"
#include "lock/locks/spsc_queue.h"
#include "lock/locks/lane_layout.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <exception>
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
    std::atomic<uint64_t> acquire_rx{0}, release_rx{0};
    std::atomic<uint64_t> responses_sent{0}, send_cqe{0};
    std::atomic<bool> stopped{false};
    std::atomic<bool> failed{false};
    std::atomic<bool> failure_claimed{false};
    std::exception_ptr failure;
    void report_failure(std::exception_ptr error) {
        bool expected = false;
        if (failure_claimed.compare_exchange_strong(
                expected, true, std::memory_order_acq_rel)) {
            failure = error;
            failed.store(true, std::memory_order_release);
        }
    }
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
void replication_loop(Shared& s, rdma::Transport& net,
                      const std::vector<NodeId>& replicas,
                      size_t lock_count, size_t window, bool profile,
                      size_t lane, size_t lane_count) {
    try {
        MuCompactLog log(net, replicas, window, 0, lane, lane_count);
        MuState state(lock_count);
        std::vector<rdma::Completion> completions;
        completions.reserve(64);
        auto profile_start = Clock::now();
        uint64_t loops = 0, commits = 0, replication_cqe = 0;
        size_t max_inflight = 0, max_pending = 0;
        while (!s.stopped.load(std::memory_order_acquire)) {
            // Only this thread touches the replica QPs and the replication CQ.
            net.poll_replication_into(completions, 64, lane);
            for (const auto& completion : completions) {
                if (completion.kind != rdma::CompletionKind::Write)
                    throw std::runtime_error("unexpected completion on Mu replication CQ");
                log.on_write_complete(completion.peer, completion.work_id);
                ++replication_cqe;
            }
            // Apply strictly contiguous quorum-committed slots. Publishing the
            // resulting actions to the communication thread is a release store.
            while (auto committed = log.next_committed()) {
                ++commits;
                for (const auto& action : state.apply(committed->command))
                    if (!s.responses.try_push(action))
                        throw std::runtime_error("Mu response queue capacity exceeded");
            }
            Message request{};
            max_pending = std::max(max_pending, s.requests.size());
            size_t admitted = 0;
            while (admitted++ < 64 && log.inflight() < window &&
                   s.requests.try_pop(request)) {
                if (lock_lane(request.lock_id, lane_count) != lane)
                    throw std::runtime_error("Mu request routed to wrong lane");
                log.append(request);
            }
            max_inflight = std::max(max_inflight, log.inflight());

            if (profile && ((++loops & 4095ULL) == 0)) {
                const auto now = Clock::now();
                const double seconds = std::chrono::duration<double>(now - profile_start).count();
                if (seconds >= 1.0) {
                    const auto acquire = s.acquire_rx.exchange(0);
                    const auto release = s.release_rx.exchange(0);
                    const auto sent = s.responses_sent.exchange(0);
                    const auto sends = s.send_cqe.exchange(0);
                    if (acquire || release || commits) {
                        std::lock_guard<std::mutex> output_lock(profile_output_mutex);
                        std::cout << "LEADER_PROFILE"
                                  << " lane=" << lane
                                  << " seconds=" << seconds
                                  << " acquire_rx=" << acquire
                                  << " release_rx=" << release
                                  << " commits=" << commits
                                  << " replication_write_cqe=" << replication_cqe
                                  << " send_cqe=" << sends
                                  << " responses_sent=" << sent
                                  << " max_inflight=" << max_inflight
                                  << " window=" << window
                                  << " max_pending=" << max_pending
                                  << std::endl;
                    }
                    profile_start = now;
                    commits = replication_cqe = 0;
                    max_inflight = max_pending = 0;
                }
            }
        }
    } catch (...) {
        s.report_failure(std::current_exception());
    }
}
} // namespace

void mu_communication_loop(
    Shared& s, const std::vector<std::unique_ptr<Shared>>& lanes,
    rdma::Transport& net, NodeId self,
    const std::vector<NodeId>& clients, size_t lock_count,
    bool profile, size_t lane, size_t lane_count,
    std::atomic<bool>& started) {
    // Each worker owns client-facing QPs and CQ for exactly one lane.
    // Only lane zero runs Ready/Stop barriers.
    std::vector<rdma::Completion> completions;
    completions.reserve(64);
    std::set<NodeId> ready, finished;
    while (!s.stopped.load(std::memory_order_acquire)) {
        if (lane == 0) {
            for (const auto& shard : lanes)
                if (shard->failed.load(std::memory_order_acquire))
                    std::rethrow_exception(shard->failure);
        }
        net.poll_communication_into(completions, 64, lane);
        for (const auto& c : completions) {
            if (c.kind == rdma::CompletionKind::Send) {
                if (profile) s.send_cqe.fetch_add(1, std::memory_order_relaxed);
                continue;
            }
            if (c.kind != rdma::CompletionKind::Receive || c.lane != lane)
                throw std::runtime_error("Mu receive on wrong communication lane");
            const Message& m = c.message;
            if (m.sender != c.peer)
                throw std::runtime_error("Mu client sender mismatch");
            const Kind kind = message_kind(m);
            if (kind == Kind::Ready && lane == 0 &&
                !started.load(std::memory_order_acquire)) {
                if (std::find(clients.begin(), clients.end(), c.peer) == clients.end() ||
                    !ready.insert(c.peer).second)
                    throw std::runtime_error("invalid Mu readiness");
                if (ready.size() == clients.size()) {
                    started.store(true, std::memory_order_release);
                    for (NodeId id : clients)
                        net.send(id, message(Kind::Ready, self), 0, true, 0);
                }
            } else if (kind == Kind::Stop && lane == 0 &&
                       started.load(std::memory_order_acquire)) {
                if (std::find(clients.begin(), clients.end(), c.peer) == clients.end() ||
                    !finished.insert(c.peer).second)
                    throw std::runtime_error("invalid Mu shutdown");
                if (finished.size() == clients.size())
                    for (NodeId id : clients)
                        net.send(id, message(Kind::Stop, self), 0, true, 0);
            } else if ((kind == Kind::Acquire || kind == Kind::Release) &&
                       started.load(std::memory_order_acquire)) {
                if (std::find(clients.begin(), clients.end(), c.peer) == clients.end() ||
                    request_node(m.request) != c.peer || m.lock_id >= lock_count ||
                    lock_lane(m.lock_id, lane_count) != lane)
                    throw std::runtime_error("Mu mutation routed to wrong lane");
                (void)pack_mu_command(m);
                if (!s.requests.try_push(m))
                    throw std::runtime_error("Mu lane request queue capacity exceeded");
                if (profile) {
                    if (kind == Kind::Acquire)
                        s.acquire_rx.fetch_add(1, std::memory_order_relaxed);
                    else
                        s.release_rx.fetch_add(1, std::memory_order_relaxed);
                }
            } else {
                throw std::runtime_error("unexpected Mu lane message");
            }
        }
        Action action{};
        size_t sent = 0;
        while (sent++ < 64 && s.responses.try_pop(action)) {
            if (lock_lane(action.message.lock_id, lane_count) != lane)
                throw std::runtime_error("Mu response routed to wrong lane");
            action.message.sender = self;
            net.send(action.destination, action.message, 0, false, lane);
            if (profile)
                s.responses_sent.fetch_add(1, std::memory_order_relaxed);
        }
    }
}

void run_mu_leader(rdma::Transport& net, NodeId self,
                   const std::vector<NodeId>& replicas,
                   const std::vector<NodeId>& clients, size_t lock_count,
                   size_t inflight_window, bool profile,
                   size_t lane_count) {
    if (lane_count < 1 || lane_count > 8)
        throw std::invalid_argument("invalid Mu lane count");
    std::vector<std::unique_ptr<Shared>> lanes;
    std::vector<std::thread> workers;
    lanes.reserve(lane_count);
    workers.reserve(2 * lane_count - 1);
    for (size_t lane = 0; lane < lane_count; ++lane)
        lanes.push_back(std::make_unique<Shared>());
    JoinWorkers join{lanes, workers};
    std::atomic<bool> started{false};
    for (size_t lane = 0; lane < lane_count; ++lane) {
        Shared* shard = lanes[lane].get();
        workers.emplace_back([&, shard, lane] {
            replication_loop(*shard, net, replicas, lock_count, inflight_window,
                             profile, lane, lane_count);
        });
    }
    for (size_t lane = 1; lane < lane_count; ++lane) {
        Shared* shard = lanes[lane].get();
        workers.emplace_back([&, shard, lane] {
            try {
                mu_communication_loop(*shard, lanes, net, self,
                    clients, lock_count, profile, lane, lane_count, started);
            } catch (...) {
                shard->report_failure(std::current_exception());
            }
        });
    }
    // Lane zero's control loop owns the start/stop barrier.
    mu_communication_loop(*lanes.front(), lanes, net, self, clients,
                    lock_count, profile, 0, lane_count, started);
}
} // namespace lock
