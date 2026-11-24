#include "lock/locks/mu_leader_runtime.h"
#include <algorithm>
#include <chrono>
#include <cstring>
#include <csignal>
#include <iostream>
#include <set>
#include <thread>
#include <unordered_set>

namespace lock {
namespace {
using Clock = std::chrono::steady_clock;
volatile std::sig_atomic_t promote_requested = 0;
void request_promotion(int) { promote_requested = 1; }
uint64_t ns(Clock::duration d) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(d).count();
}
void replay_to(MuRecoveryState& state, const MuLogView& view, uint64_t end) {
    while (state.applied() < end) state.apply(view.command(state.applied()));
}
void check_deadline(Clock::time_point deadline) {
    if (Clock::now() >= deadline) throw std::runtime_error("Mu recovery watchdog");
}
// No outstanding normal replication WRs on the standby's surviving peer QP.
void wait_completion(rdma::Transport& net, NodeId peer, WorkId id,
                     rdma::CompletionKind kind, Clock::time_point deadline) {
    std::vector<rdma::Completion> events;
    events.reserve(64);
    while (true) {
        net.poll_replication_into(events);
        for (auto& e : events) {
            if (e.peer != peer || e.kind != kind || e.work_id != id)
                throw std::runtime_error("unexpected recovery transfer completion");
            return;
        }
        check_deadline(deadline);
    }
}
}
void install_mu_recovery_trigger() {
    if (std::signal(SIGUSR1, request_promotion) == SIG_ERR)
        throw std::runtime_error("cannot install recovery trigger");
}
void run_mu_standby(rdma::Transport& net, const std::vector<NodeId>& replicas,
                   const std::vector<NodeId>& clients, size_t locks, size_t window,
                   bool profile, size_t replication_batch, size_t communication_batch,
                   MuRecoveryConfig recovery, size_t watchdog_seconds) {
    if (!recovery.enabled || replicas.size() != 3 ||
        net.self() == replicas.front())
        throw std::invalid_argument("invalid Mu standby configuration");
    MuLogView view(net, recovery.publication);
    auto shadow = std::make_unique<MuRecoveryState>(locks, view.capacity() + 512);
    uint64_t frontier = 0;
    // Poll only the next contiguous slot. With background replay disabled,
    // maintain the same cheap frontier scan but defer all state application.
    // The trigger is a controller notification, not a failure detector.
    while (!promote_requested) {
        frontier = view.scan(frontier, 256);
        if (recovery.replay)
            replay_to(*shadow, view, safe_replay_prefix(frontier, window));
    }
    const auto begin = Clock::now();
    const auto deadline = begin + std::chrono::seconds(watchdog_seconds);
    const uint64_t replayed_at_trigger = shadow->applied();
    net.fence_peer(replicas.front());
    const auto fenced = Clock::now();
    frontier = view.scan(frontier);
    const auto scanned = Clock::now();
    const NodeId replacement = replicas[1], other = replicas[2];
    if (net.self() == other) {
        // Certify a stable contiguous length only after local fencing.
        Message seal = message(Kind::RecoverySeal, other, 0, 0, kReplacementEpoch);
        seal.position = frontier;
        seal.value = shadow->applied();
        net.send(replacement, seal, 1);
        wait_completion(net, replacement, 1, rdma::CompletionKind::Send, deadline);
        std::cout << "MU_RECOVERY_SEAL node=" << other << " length=" << frontier
                  << " replayed=" << replayed_at_trigger << " fence_ns=" << ns(fenced-begin)
                  << " scan_ns=" << ns(scanned-fenced) << std::endl;
        // No second failure or replica restart in this experiment.
        while (true) std::this_thread::sleep_for(std::chrono::hours(24));
    }
    if (net.self() != replacement) throw std::runtime_error("unexpected standby identity");
    std::vector<rdma::Completion> events;
    events.reserve(64);
    uint64_t remote = 0;
    bool sealed = false;
    while (!sealed) {
        net.poll_replication_into(events);
        for (const auto& e : events) {
            if (e.kind != rdma::CompletionKind::Receive || e.peer != other ||
                e.message.sender != other || message_kind(e.message) != Kind::RecoverySeal ||
                e.message.epoch != kReplacementEpoch ||
                e.message.position > view.capacity() || e.message.value > e.message.position ||
                sealed)
                throw std::runtime_error("invalid standby seal");
            remote = e.message.position;
            sealed = true;
        }
        check_deadline(deadline);
    }
    const auto sealed_at = Clock::now();
    const uint64_t recovered = std::max(frontier, remote);
    const uint64_t common = std::min(frontier, remote);
    // Validate the overlapping uncertain tail. Earlier entries belong to
    // one immutable history by assumption; do not re-read the full log.
    const size_t overlap = std::min<uint64_t>(common, window);
    WorkId work = 2;
    if (overlap) {
        auto* scratch = static_cast<uint64_t*>(net.allocate(overlap * 8, 8));
        net.read_range(other, scratch, overlap * 8,
            rdma::kLogOffset + (common - overlap) * 8, work);
        wait_completion(net, other, work++, rdma::CompletionKind::Read, deadline);
        for (size_t i = 0; i < overlap; ++i)
            if (scratch[i] != view.word(common - overlap + i))
                throw std::runtime_error("conflicting surviving log histories");
    }
    // Repair only the shorter suffix. The fixed majority is R2 + R3.
    constexpr uint64_t chunk_words = 128 * 1024;
    for (uint64_t i = common; i < recovered;) {
        const size_t count = std::min<uint64_t>(chunk_words, recovered - i);
        const size_t offset = rdma::kLogOffset + i * 8;
        if (frontier < remote) {
            net.read_range(other, view.words(i, count), count * 8, offset, work);
            wait_completion(net, other, work++, rdma::CompletionKind::Read, deadline);
            if (recovery.publication) std::memset(view.marks(i, count), 1, count);
        } else {
            net.write(other, view.words(i, count), count * 8, offset, work);
            wait_completion(net, other, work++, rdma::CompletionKind::Write, deadline);
            if (recovery.publication) {
                net.write(other, view.marks(i, count), count,
                          mu_recovery_marker_offset(net.memory_bytes(), i), work);
                wait_completion(net, other, work++, rdma::CompletionKind::Write, deadline);
            }
        }
        i += count;
    }
    const auto repaired = Clock::now();
    // Both surviving replicas now have the same immutable prefix. Establish
    // the next epoch on that fixed majority before authorizing any client.
    const auto boundary = encode_epoch_boundary({kReplacementEpoch, recovered});
    const auto boundary_offset = epoch_boundary_offset(net.memory_bytes(), kReplacementEpoch);
    auto* local_boundary = static_cast<uint64_t*>(net.at(boundary_offset, sizeof(uint64_t)));
    if (*local_boundary != 0) throw std::runtime_error("epoch boundary already exists");
    if (!net.write_inline(other, boundary, boundary_offset, work))
        throw std::runtime_error("epoch boundary WRITE must be signaled");
    wait_completion(net, other, work++, rdma::CompletionKind::Write, deadline);
    *local_boundary = boundary;
    const auto epoch_committed = Clock::now();
    replay_to(*shadow, view, recovered);
    const auto replayed = Clock::now();
    // All old requests and responses are uncertain until clients report
    // their actual phases. No normal new-leader traffic before Ready.
    for (NodeId c : clients) net.send(c, message(Kind::RecoveryBegin, replacement, 0, 0, kReplacementEpoch));
    std::set<NodeId> finished;
    std::unordered_set<RequestId> reported;
    size_t active_reported = 0;
    std::vector<Message> pending;
    std::vector<Action> responses;
    while (finished.size() != clients.size()) {
        net.poll_communication_into(events);
        for (const auto& e : events) {
            if (e.kind == rdma::CompletionKind::Send && e.work_id == 0) continue;
            if (e.kind != rdma::CompletionKind::Receive || e.lane != 0 ||
                std::find(clients.begin(), clients.end(), e.peer) == clients.end() ||
                e.message.sender != e.peer || e.message.epoch != kReplacementEpoch || finished.contains(e.peer))
                throw std::runtime_error("invalid client recovery event");
            const auto& m = e.message;
            auto k = message_kind(m);
            if (k == Kind::RecoveryDone) {
                finished.insert(e.peer);
                continue;
            }
            if ((k != Kind::RecoveryAcquire && k != Kind::RecoveryRelease) ||
                request_node(m.request) != e.peer || m.lock_id >= locks ||
                !reported.insert(m.request).second)
                throw std::runtime_error("invalid client recovery report");
            const auto info = shadow->lookup(m.request);
            if (info.status == MuRequestStatus::Waiting || info.status == MuRequestStatus::Granted)
                ++active_reported;
            switch (reconcile_mu(info, m.lock_id, k == Kind::RecoveryRelease)) {
            case MuRecoveryAction::RetryAcquire:
                pending.push_back(message(Kind::Acquire, e.peer, m.lock_id, m.request)); break;
            case MuRecoveryAction::RetryRelease:
                pending.push_back(message(Kind::Release, e.peer, m.lock_id, m.request)); break;
            case MuRecoveryAction::Grant:
                responses.push_back({e.peer, message(Kind::Grant, 0, m.lock_id, m.request)}); break;
            case MuRecoveryAction::Ack:
                responses.push_back({e.peer, message(Kind::ReleaseAck, 0, m.lock_id, m.request)}); break;
            case MuRecoveryAction::Wait: break;
            }
        }
        check_deadline(deadline);
    }
    if (active_reported != shadow->active())
        throw std::runtime_error("client recovery reports omit live requests");
    MuPromotion promotion{shadow->take_state(), std::move(pending), std::move(responses)};
    shadow.reset();
    const auto reconciled = Clock::now();
    for (NodeId c : clients) net.send(c, message(Kind::RecoveryReady, replacement, 0, 0, kReplacementEpoch));
    const auto ready = Clock::now();
    std::cout << "MU_RECOVERY {"
              << "\"node\":" << replacement << ",\"local_length\":" << frontier
              << ",\"remote_length\":" << remote << ",\"recovered_length\":" << recovered
              << ",\"replayed_at_trigger\":" << replayed_at_trigger
              << ",\"replay_entries\":" << recovered - replayed_at_trigger
              << ",\"repair_entries\":" << recovered - common
              << ",\"fence_ns\":" << ns(fenced-begin)
              << ",\"scan_ns\":" << ns(scanned-fenced)
              << ",\"seal_wait_ns\":" << ns(sealed_at-scanned)
              << ",\"repair_ns\":" << ns(repaired-sealed_at)
              << ",\"epoch\":" << kReplacementEpoch
              << ",\"epoch_first_slot\":" << recovered
              << ",\"epoch_commit_ns\":" << ns(epoch_committed-repaired)
              << ",\"replay_ns\":" << ns(replayed-epoch_committed)
              << ",\"reconcile_ns\":" << ns(reconciled-replayed)
              << ",\"ready_send_ns\":" << ns(ready-reconciled)
              << ",\"recovery_ns\":" << ns(ready-begin) << "}" << std::endl;
    recovery.recovered = recovered;
    recovery.promoted = true;
    run_mu_leader(net, replacement, {replacement, other}, clients, locks, window,
                  profile, 1, replication_batch, communication_batch, recovery, &promotion);
}
} // namespace lock
