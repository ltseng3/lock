#include "lock/config.h"
#include "lock/locks/backend.h"
#include "lock/locks/handoff_state.h"
#include <atomic>
#include <cstddef>
#include "lock/locks/mu_state.h"
#include "lock/locks/mu_leader_runtime.h"
#include "lock/locks/handoff_leader_runtime.h"
#include <thread>
#include "lock/locks/replicated_log.h"
#include "lock/rdma/transport.h"
#include <algorithm>
#include <chrono>
#include <iostream>
#include <memory>
#include <set>
#include <stdexcept>
#include <vector>

namespace {
std::vector<lock::NodeId> ids(const std::vector<std::string>& list) {
    std::vector<lock::NodeId> out;
    for (const auto& item : list) {
        auto id = std::stoul(item);
        if (id == 0 || id > 255) throw std::invalid_argument("invalid node ID");
        out.push_back(id);
    }
    return out;
}
}
int main(int argc, char** argv) {
    try {
        lock::Options opt(argc, argv);
        lock::rdma::Config cfg;
        cfg.self = opt.number("node-id", 0);
        cfg.peers = lock::rdma::parse_peers(opt.require("peers"));
        cfg.tcp_port = opt.number("tcp-port", 18515);
        cfg.ib_port = opt.number("ib-port", 1);
        cfg.device = opt.get("device");
        cfg.memory_bytes = opt.number("memory-mib", 512) * 1024ULL * 1024;
        auto replicas = ids(opt.list("replicas"));
        auto clients = ids(opt.list("clients"));
        auto backend = lock::parse_backend(opt.require("backend"));
        const bool profile_requested = opt.number("profile-leader", 0) != 0;
        const auto gc = opt.number("handoff-gc", 0);
        if (gc > 1 || (gc && backend != lock::Backend::Handoff))
            throw std::invalid_argument("handoff-gc requires handoff and 0 or 1");
        const lock::HandoffLeaseConfig lease{opt.number("handoff-lease-us", 0),
            opt.number("handoff-lease-scan-us", 0), opt.number("handoff-heartbeat-us", 0)};
        lease.validate();
        if ((lease.duration_us || lease.heartbeat_us) && backend != lock::Backend::Handoff)
            throw std::invalid_argument("lease monitor requires Handoff");
        const size_t lock_count = opt.number("locks", 1000);
        const size_t mu_inflight_window = opt.number("mu-inflight-window", 512);
        if (mu_inflight_window < 1 || mu_inflight_window > 512)
            throw std::invalid_argument("mu-inflight-window must be 1..512");
        const size_t handoff_inflight_window = opt.number("handoff-inflight-window", 512);
        if (handoff_inflight_window < 1 || handoff_inflight_window > 512)
            throw std::invalid_argument("handoff-inflight-window must be 1..512");
        const size_t append_window =
            backend == lock::Backend::Mu ? mu_inflight_window : 128;
        if (std::find(replicas.begin(), replicas.end(), cfg.self) == replicas.end())
            throw std::invalid_argument("server node is not a replica");
        const size_t mu_lanes = opt.number("mu-lanes", 1);
        if (mu_lanes < 1 || mu_lanes > 8)
            throw std::invalid_argument("mu-lanes must be 1..8");
        if (backend == lock::Backend::Cas && mu_lanes != 1)
            throw std::invalid_argument("CAS does not use Mu lanes");
        const size_t replication_batch = opt.number("replication-batch", 8);
        cfg.communication_batch = opt.number("leader-comm-batch", 64);
        if (!replication_batch || replication_batch > 16 ||
            !cfg.communication_batch || cfg.communication_batch > 64)
            throw std::invalid_argument("invalid leader batch size");
        const bool leader = cfg.self == replicas.front();
        lock::MuRecoveryConfig recovery;
        const auto recover = opt.number("mu-recovery", 0);
        const auto publication = opt.number("mu-recovery-publication", 1);
        const auto replay = opt.number("mu-recovery-replay", 1);
        if (recover > 1 || publication > 1 || replay > 1 ||
            (recover && (backend != lock::Backend::Mu || replicas.size() != 3 ||
                         mu_lanes != 1 || clients.size() > 15)))
            throw std::invalid_argument("Mu recovery requires 3 replicas, one lane, <=15 clients");
        recovery.enabled = recover != 0;
        recovery.publication = publication != 0;
        recovery.replay = replay != 0;
        if (recovery.enabled && !leader) {
            cfg.recoverable_peer = replicas.front();
            lock::install_mu_recovery_trigger();
        }
        if (backend == lock::Backend::Mu ||
            backend == lock::Backend::Handoff) {
            // Every replica must bootstrap identical per-lane QPs, including
            // passive followers, for the leader's one-sided inline WRITEs.
            cfg.replication_peers = replicas;
            cfg.replication_lanes = mu_lanes;
            if (leader || (recovery.enabled && cfg.self == replicas[1]))
                cfg.client_lane_peers = clients;
        }
        lock::rdma::Transport net(cfg);
        net.connect();
        std::cout << "READY node=" << cfg.self << std::endl;
        if (backend == lock::Backend::Mu || backend == lock::Backend::Handoff) {
            if (leader) {
                if (backend == lock::Backend::Mu)
                    lock::run_mu_leader(net, cfg.self, replicas, clients,
                                        lock_count, mu_inflight_window,
                                        profile_requested, mu_lanes, replication_batch,
                                             cfg.communication_batch, recovery);
                else
                    lock::run_handoff_leader(net, cfg.self, replicas, clients,
                                             lock_count, opt.number("prepared-window", 8),
                                             handoff_inflight_window,
                                             profile_requested, mu_lanes, replication_batch,
                                             cfg.communication_batch, gc != 0, lease);
            } else if (recovery.enabled) {
                lock::run_mu_standby(net, replicas, clients, lock_count, mu_inflight_window,
                    profile_requested, replication_batch, cfg.communication_batch, recovery,
                    opt.number("watchdog-seconds", 90));
            } else {
                // Follower NICs accept one-sided header WRITEs autonomously.
                // No application-side polling in the measured path.
                while (true) std::this_thread::sleep_for(std::chrono::hours(24));
            }
            return 0;
        }
        const bool profile_mu = profile_requested && leader && backend == lock::Backend::Mu;
        using Clock = std::chrono::steady_clock;
        auto profile_start = Clock::now();
        uint64_t profile_loops = 0;
        uint64_t acquire_rx = 0, release_rx = 0, write_cqe = 0;
        uint64_t send_cqe = 0, committed_count = 0, sent_responses = 0;
        size_t max_inflight = 0, max_pending = 0;
        std::vector<lock::rdma::Completion> completions;
        completions.reserve(64);
        std::set<lock::NodeId> ready;
        std::set<lock::NodeId> finished;
        bool started = false;
        std::unique_ptr<lock::ReplicatedLog> log;
        std::unique_ptr<lock::MuState> mu;
        std::unique_ptr<lock::HandoffState> handoff;
        if (leader && backend != lock::Backend::Cas) {
            log = std::make_unique<lock::ReplicatedLog>(
                net, replicas, backend == lock::Backend::Mu);
            if (backend == lock::Backend::Mu)
                mu = std::make_unique<lock::MuState>(lock_count);
            else
                handoff = std::make_unique<lock::HandoffState>(
                    lock_count, opt.number("prepared-window", 8));
        }
        std::vector<lock::Message> pending;
        size_t head = 0;
        while (true) {
            net.poll_into(completions);
            for (auto& event : completions) {
                if (event.kind == lock::rdma::CompletionKind::Write) {
                    if (!leader || !log) throw std::runtime_error("unexpected server WRITE CQE");
                    log->on_write_complete(event.work_id);
                    if (profile_mu) ++write_cqe;
                    continue;
                }
                if (event.kind == lock::rdma::CompletionKind::Send) {
                    if (profile_mu) ++send_cqe;
                    continue;
                }
                if (event.kind != lock::rdma::CompletionKind::Receive) continue;
                const auto& m = event.message;
                if (m.sender != event.peer) throw std::runtime_error("spoofed sender ID");
                auto kind = lock::message_kind(m);
                if (!leader) throw std::runtime_error("unexpected follower RPC");
                if (kind == lock::Kind::Ready) {
                    if (std::find(clients.begin(), clients.end(), event.peer) == clients.end())
                        throw std::runtime_error("non-client readiness");
                    ready.insert(event.peer);
                    if (ready.size() == clients.size() && !started) {
                        started = true;
                        for (auto peer : clients)
                            net.send(peer, lock::message(lock::Kind::Ready, cfg.self));
                    }
                } else if (kind == lock::Kind::Stop && started) {
                    if (std::find(clients.begin(), clients.end(), event.peer) == clients.end() ||
                        !finished.insert(event.peer).second)
                        throw std::runtime_error("invalid or repeated client shutdown");
                    if (finished.size() == clients.size()) {
                        // Client Stop is sent only after all its active operations
                        // and one-sided release writes have finished. The leader
                        // has already acknowledged every retired position.
                        for (auto peer : clients)
                            net.send(peer, lock::message(lock::Kind::Stop, cfg.self));
                    }
                } else if ((kind == lock::Kind::Acquire || kind == lock::Kind::Release) &&
                           started && backend != lock::Backend::Cas) {
                    if (std::find(clients.begin(), clients.end(), event.peer) == clients.end() ||
                        lock::request_node(m.request) != event.peer ||
                        m.lock_id >= lock_count)
                        throw std::runtime_error("invalid mutation request");
                    if (handoff) {
                        if (kind != lock::Kind::Acquire)
                            throw std::runtime_error("handoff accepts only Acquire");
                        auto step = handoff->admit(m);
                        for (auto& command : step.append) pending.push_back(command);
                    } else {
                        pending.push_back(m);
                        if (profile_mu) {
                            if (kind == lock::Kind::Acquire) ++acquire_rx;
                            else ++release_rx;
                        }
                    }
                } else {
                    throw std::runtime_error("unexpected leader request");
                }
            }
            if (!leader || !log) continue;
            // Bound in-flight log work; Mu has a configurable throughput window.
            if (profile_mu) max_pending = std::max(max_pending, pending.size() - head);
            while (head < pending.size() && log->inflight() < append_window) {
                log->append(pending[head++]);
            }
            if (profile_mu) max_inflight = std::max(max_inflight, log->inflight());
            if (head == pending.size()) { pending.clear(); head = 0; }
            while (auto committed = log->next_committed()) {
                if (profile_mu) ++committed_count;
                if (mu) for (auto& action : mu->apply(committed->command)) {
                    action.message.sender = cfg.self;
                    // Mu responses need reliable RC delivery, not per-SEND CQEs.
                    // Transport forces a signaled SEND at least every 16 WRs/QP.
                    net.send(action.destination, action.message, 0, false);
                    if (profile_mu) ++sent_responses;
                }
                if (handoff) for (auto& action : handoff->committed(*committed)) {
                    action.message.sender = cfg.self;
                    net.send(action.destination, action.message);
                }
            }
            if (handoff) {
                auto step = handoff->retire([&](uint64_t slot) -> uint64_t {
                    size_t offset = lock::rdma::kLogOffset +
                                    slot * sizeof(lock::LogRecord) +
                                    offsetof(lock::LogRecord, retired);
                    auto* ptr = static_cast<uint64_t*>(net.at(offset, sizeof(uint64_t)));
                    return std::atomic_ref<uint64_t>(*ptr).load(std::memory_order_acquire);
                });
                for (auto& action : step.actions) {
                    action.message.sender = cfg.self;
                    net.send(action.destination, action.message);
                }
                for (auto& command : step.append) pending.push_back(command);
            }
            if (profile_mu && ((++profile_loops & 4095ULL) == 0)) {
                const auto now = Clock::now();
                const auto elapsed = std::chrono::duration<double>(now - profile_start).count();
                if (elapsed >= 1.0) {
                    if (acquire_rx || release_rx || committed_count) {
                        std::cout << "LEADER_PROFILE"
                                  << " seconds=" << elapsed
                                  << " acquire_rx=" << acquire_rx
                                  << " release_rx=" << release_rx
                                  << " commits=" << committed_count
                                  << " replication_write_cqe=" << write_cqe
                                  << " send_cqe=" << send_cqe
                                  << " responses_sent=" << sent_responses
                                  << " max_inflight=" << max_inflight
                                  << " window=" << append_window
                                  << " max_pending=" << max_pending
                                  << std::endl;
                    }
                    profile_start = now;
                    acquire_rx = release_rx = write_cqe = send_cqe = 0;
                    committed_count = sent_responses = 0;
                    max_inflight = max_pending = 0;
                }
            }
        }
    } catch (const std::exception& e) {
        std::cerr << "ERROR " << e.what() << std::endl;
        return 1;
    }
}
