#include "lock/locks/mu_leader_runtime.h"
#include "lock/locks/compact_recovery_runtime.h"
#include <algorithm>
#include <memory>
#include <set>
#include <unordered_set>

namespace lock {
void run_mu_standby(rdma::Transport& net, const std::vector<NodeId>& replicas,
                   const std::vector<NodeId>& clients, size_t locks, size_t window,
                   bool profile, size_t replication_batch, size_t communication_batch,
                   CompactRecoveryConfig recovery, size_t watchdog_seconds) {
    if (!recovery.enabled || replicas.size() != 3 ||
        net.self() == replicas.front())
        throw std::invalid_argument("invalid Mu standby configuration");
    CompactLogView view(net, recovery.publication);
    auto shadow = std::make_unique<MuRecoveryState>(locks, view.capacity() + 512);
    const auto p = recover_compact_prefix(net, replicas, window, recovery, view,
        [&](uint64_t end) {
            while (shadow->applied() < end) shadow->apply(view.command(shadow->applied()));
        }, watchdog_seconds);
    const NodeId replacement = replicas[1], other = replicas[2];
    std::vector<rdma::Completion> events;
    events.reserve(64);
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
        check_recovery_deadline(p.deadline);
    }
    if (active_reported != shadow->active())
        throw std::runtime_error("client recovery reports omit live requests");
    MuPromotion promotion{shadow->take_state(), std::move(pending), std::move(responses)};
    shadow.reset();
    const auto reconciled = RecoveryClock::now();
    for (NodeId c : clients) net.send(c, message(Kind::RecoveryReady, replacement, 0, 0, kReplacementEpoch));
    const auto ready = RecoveryClock::now();
    print_recovery_result("MU_RECOVERY", replacement, p, reconciled, ready);
    recovery.recovered = p.length;
    recovery.promoted = true;
    run_mu_leader(net, replacement, {replacement, other}, clients, locks, window,
                  profile, 1, replication_batch, communication_batch, recovery, &promotion);
}
} // namespace lock
