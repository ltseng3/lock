#include "lock/locks/handoff_leader_runtime.h"
#include "lock/locks/handoff_layout.h"
#include "lock/locks/handoff_state.h"
#include "lock/locks/mu_compact_log.h"
#include "lock/locks/mu_encoding.h"
#include "lock/locks/spsc_queue.h"
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <deque>
#include <exception>
#include <iostream>
#include <set>
#include <stdexcept>
#include <thread>
#include <vector>

namespace lock {
namespace {
using Clock = std::chrono::steady_clock;
struct Shared {
    SpscQueue<Message> requests;
    SpscQueue<Action> responses;
    std::atomic<uint64_t> acquire_rx{0}, responses_sent{0}, send_cqe{0};
    std::atomic<bool> stopped{false}, failed{false};
    std::exception_ptr failure;
};
struct JoinWorker {
    Shared& shared;
    std::thread& worker;
    ~JoinWorker() {
        shared.stopped.store(true, std::memory_order_release);
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
                      size_t window, bool profile) {
    try {
        // Half the registered log area holds 8-byte replicated headers. The
        // adjacent half holds one 8-byte marker per slot, written by the
        // completed client to the leader's NIC-exposed memory.
        const size_t capacity = handoff_log_capacity(net.memory_bytes());
        MuCompactLog log(net, replicas, window, capacity);
        HandoffState state(lock_count, prepared_window);
        std::deque<Message> append_pending;
        std::vector<rdma::Completion> cqes;
        cqes.reserve(64);
        auto profile_start = Clock::now();
        uint64_t loops = 0, commits = 0, write_cqe = 0, retire_reads = 0;
        uint64_t emitted = 0;
        size_t max_inflight = 0, max_pending = 0;
        while (!s.stopped.load(std::memory_order_acquire)) {
            net.poll_replication_into(cqes);
            for (const auto& c : cqes) {
                if (c.kind != rdma::CompletionKind::Write)
                    throw std::runtime_error("unexpected handoff replication CQE");
                log.on_write_complete(c.peer, c.work_id);
                ++write_cqe;
            }

            // Commit and emit ORDER/NEXT/START in the global log order.
            while (auto committed = log.next_committed()) {
                ++commits;
                for (const auto& action : state.committed(*committed)) {
                    submit_action(s, action);
                    ++emitted;
                }
            }

            // Only inspect locks with committed, not-yet-retired entries.
            // Retirement evidence is a client one-sided WRITE to the marker
            // sidecar; the leader does not need an RPC for release.
            auto retired = state.retire([&](uint64_t slot) {
                ++retire_reads;
                const size_t offset = handoff_marker_offset(net.memory_bytes(), slot);
                auto* ptr = static_cast<uint64_t*>(net.at(offset, sizeof(uint64_t)));
                return std::atomic_ref<uint64_t>(*ptr).load(std::memory_order_acquire);
            });
            for (const auto& action : retired.actions) {
                submit_action(s, action);
                ++emitted;
            }
            for (const auto& cmd : retired.append)
                append_pending.push_back(cmd);

            Message request{};
            // This thread exclusively owns the per-lock queue and reservation
            // state. Client receives never mutate those structures directly.
            while (s.requests.try_pop(request)) {
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
                        std::cout << "HANDOFF_PROFILE"
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
                        size_t inflight_window, bool profile) {
    Shared shared;
    std::thread worker([&] {
        replication_loop(shared, net, replicas, lock_count, prepared_window,
                         inflight_window, profile);
    });
    JoinWorker join{shared, worker};

    // The communication thread exclusively owns client RC QPs and their CQ.
    // The worker owns follower QPs and the separate replication CQ.
    std::vector<rdma::Completion> cqes;
    cqes.reserve(64);
    std::set<NodeId> ready, finished;
    bool started = false;
    while (true) {
        if (shared.failed.load(std::memory_order_acquire))
            std::rethrow_exception(shared.failure);
        net.poll_into(cqes);
        for (const auto& c : cqes) {
            if (c.kind == rdma::CompletionKind::Send) {
                if (profile) shared.send_cqe.fetch_add(1, std::memory_order_relaxed);
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
                if (!shared.requests.try_push(m))
                    throw std::runtime_error("handoff request queue capacity exceeded");
                if (profile)
                    shared.acquire_rx.fetch_add(1, std::memory_order_relaxed);
            } else {
                throw std::runtime_error("unexpected handoff client message");
            }
        }

        Action action{};
        while (shared.responses.try_pop(action)) {
            action.message.sender = self;
            // RC delivery is reliable; periodically signal the send queue to
            // reclaim WRs without asking for one CQE per response.
            net.send(action.destination, action.message, 0, false);
            if (profile)
                shared.responses_sent.fetch_add(1, std::memory_order_relaxed);
        }
    }
}
} // namespace lock
