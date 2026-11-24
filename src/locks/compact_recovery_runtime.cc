#include "lock/locks/compact_recovery_runtime.h"
#include <algorithm>
#include <cstring>
#include <csignal>
#include <iostream>
#include <thread>

namespace lock {
namespace {
using Clock = std::chrono::steady_clock;
volatile std::sig_atomic_t promote_requested = 0;
void request_promotion(int) { promote_requested = 1; }
uint64_t ns(Clock::duration d) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(d).count();
}
// No outstanding normal replication WRs on the standby's surviving peer QP.
void wait_completion(rdma::Transport& net, NodeId peer, WorkId id,
                     rdma::CompletionKind kind, Clock::time_point deadline) {
    std::vector<rdma::Completion> events;
    events.reserve(64);
    while (true) {
        net.poll_replication_into(events);
        if (!events.empty()) {
            const auto& e = events.front();
            if (events.size() != 1 || e.peer != peer || e.kind != kind || e.work_id != id)
                throw std::runtime_error("unexpected recovery transfer completion");
            return;
        }
        check_recovery_deadline(deadline);
    }
}
}
void install_recovery_trigger() {
    if (std::signal(SIGUSR1, request_promotion) == SIG_ERR)
        throw std::runtime_error("cannot install recovery trigger");
}

void check_recovery_deadline(RecoveryClock::time_point deadline) {
    if (RecoveryClock::now() >= deadline) throw std::runtime_error("recovery watchdog");
}
RecoveredPrefix recover_compact_prefix(
    rdma::Transport& net, const std::vector<NodeId>& replicas, size_t window,
    CompactRecoveryConfig recovery, const CompactLogView& view,
    const std::function<void(uint64_t)>& replay_to, size_t watchdog_seconds) {
    if (!recovery.enabled || replicas.size() != 3 || !window ||
        net.self() == replicas.front())
        throw std::invalid_argument("invalid standby configuration");
    uint64_t frontier = 0, applied = 0;
    // Poll only the next contiguous slot. With background replay disabled,
    // maintain the same cheap frontier scan but defer all state application.
    // The trigger is a controller notification, not a failure detector.
    while (!promote_requested) {
        frontier = view.scan(frontier, 256);
        if (recovery.replay) {
            const auto target = safe_replay_prefix(frontier, window);
            replay_to(target);
            applied = target;
        }
    }
    const auto begin = Clock::now();
    const auto deadline = begin + std::chrono::seconds(watchdog_seconds);
    const uint64_t replayed_at_trigger = applied;
    net.fence_peer(replicas.front());
    const auto fenced = Clock::now();
    frontier = view.scan(frontier);
    const auto scanned = Clock::now();
    const NodeId replacement = replicas[1], other = replicas[2];
    if (net.self() == other) {
        // Certify a stable contiguous length only after local fencing.
        Message seal = message(Kind::RecoverySeal, other, 0, 0, kReplacementEpoch);
        seal.position = frontier;
        seal.value = applied;
        net.send(replacement, seal, 1);
        wait_completion(net, replacement, 1, rdma::CompletionKind::Send, deadline);
        std::cout << (recovery.handoff ? "HANDOFF_RECOVERY_SEAL node=" : "MU_RECOVERY_SEAL node=") << other << " length=" << frontier
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
        check_recovery_deadline(deadline);
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
                          compact_publication_offset(net.memory_bytes(), i, recovery.handoff), work);
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
    replay_to(recovered);
    const auto replayed = Clock::now();
    return {frontier, remote, recovered, replayed_at_trigger,
            begin, deadline, fenced, scanned, sealed_at, repaired, epoch_committed, replayed};
}
void print_recovery_result(const char* prefix, NodeId replacement,
    const RecoveredPrefix& p, RecoveryClock::time_point reconciled,
    RecoveryClock::time_point ready) {
    std::cout << prefix << " {"
              << "\"node\":" << replacement << ",\"local_length\":" << p.local
              << ",\"remote_length\":" << p.remote << ",\"recovered_length\":" << p.length
              << ",\"replayed_at_trigger\":" << p.replayed_at_trigger
              << ",\"replay_entries\":" << p.length - p.replayed_at_trigger
              << ",\"repair_entries\":" << p.length - std::min(p.local,p.remote)
              << ",\"fence_ns\":" << ns(p.fenced-p.begin)
              << ",\"scan_ns\":" << ns(p.scanned-p.fenced)
              << ",\"seal_wait_ns\":" << ns(p.sealed-p.scanned)
              << ",\"repair_ns\":" << ns(p.repaired-p.sealed)
              << ",\"epoch\":" << kReplacementEpoch
              << ",\"epoch_first_slot\":" << p.length
              << ",\"epoch_commit_ns\":" << ns(p.epoch_committed-p.repaired)
              << ",\"replay_ns\":" << ns(p.replayed-p.epoch_committed)
              << ",\"reconcile_ns\":" << ns(reconciled-p.replayed)
              << ",\"ready_send_ns\":" << ns(ready-reconciled)
              << ",\"recovery_ns\":" << ns(ready-p.begin) << "}" << std::endl;
}
} // namespace lock
