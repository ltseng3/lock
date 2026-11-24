#include "lock/locks/handoff_leader_runtime.h"
#include "lock/locks/compact_recovery_runtime.h"
#include <algorithm>
#include <array>
#include <memory>
#include <stdexcept>

namespace lock {
namespace {
// All clients are frozen. Install relationships in per-client FIFO order,
// before allowing execution. Each batch has one tail CQE and at most 64
// WRs per QP. Drain these CQEs before starting the ordinary response batcher.
void install_relationships(rdma::Transport& net, const std::vector<NodeId>& clients,
                           const std::vector<Action>& actions,
                           RecoveryClock::time_point deadline) {
    std::array<std::vector<Message>,256> messages;
    std::array<size_t,256> cursor{};
    for (auto a : actions) {
        if (std::find(clients.begin(), clients.end(), a.destination) == clients.end())
            throw std::runtime_error("relationship targets nonmember");
        a.message.sender = net.self();
        a.message.epoch = kReplacementEpoch;
        messages[a.destination].push_back(a.message);
    }
    std::vector<rdma::Completion> events;
    events.reserve(64);
    WorkId token = 1;
    while (true) {
        std::array<WorkId,256> pending{};
        size_t remaining = 0;
        for (auto c : clients) {
            const size_t count = std::min<size_t>(64, messages[c].size() - cursor[c]);
            if (!count) continue;
            pending[c] = token++;
            net.send_batch(c, {messages[c].data() + cursor[c], count}, pending[c], 0);
            cursor[c] += count;
            ++remaining;
        }
        if (!remaining) break;
        while (remaining) {
            net.poll_communication_into(events);
            for (const auto& e : events) {
                if (e.kind != rdma::CompletionKind::Send || e.lane != 0 ||
                    e.peer >= pending.size())
                    throw std::runtime_error("unexpected relationship install event");
                if (!e.work_id) continue; // earlier RecoveryBegin
                if (pending[e.peer] != e.work_id)
                    throw std::runtime_error("invalid relationship install completion");
                pending[e.peer] = 0;
                --remaining;
            }
            check_recovery_deadline(deadline);
        }
    }
}
}
void run_handoff_standby(rdma::Transport& net, const std::vector<NodeId>& replicas,
                         const std::vector<NodeId>& clients, size_t locks,
                         size_t prepared_window, size_t window, bool profile,
                         size_t replication_batch, size_t communication_batch,
                         CompactRecoveryConfig recovery, size_t watchdog_seconds) {
    if (!recovery.enabled || !recovery.handoff || replicas.size() != 3)
        throw std::invalid_argument("invalid handoff standby configuration");
    CompactLogView view(net, recovery.publication, 0, 1, true);
    auto index = std::make_unique<HandoffRecoveryIndex>(locks, view.capacity());
    const auto p = recover_compact_prefix(net, replicas, window, recovery, view,
        [&](uint64_t end) {
            while (index->applied() < end) index->apply(view.command(index->applied()));
        }, watchdog_seconds);
    const auto replacement = replicas[1], other = replicas[2];
    std::array<int,256> client_index;
    client_index.fill(-1);
    std::vector<HandoffClientSnapshot> snapshots;
    for (auto c : clients) {
        if (!c || c >= client_index.size() || client_index[c] >= 0)
            throw std::runtime_error("invalid handoff recovery membership");
        client_index[c] = static_cast<int>(snapshots.size());
        snapshots.push_back({c,0,{}});
        net.send(c, message(Kind::RecoveryBegin, replacement, 0, 0, kReplacementEpoch));
    }
    std::array<bool,256> finished{};
    size_t count = 0;
    std::vector<rdma::Completion> events;
    events.reserve(64);
    while (count != clients.size()) {
        net.poll_communication_into(events);
        for (const auto& e : events) {
            if (e.kind == rdma::CompletionKind::Send && !e.work_id) continue;
            if (e.kind != rdma::CompletionKind::Receive || e.lane != 0 ||
                e.peer >= client_index.size() || client_index[e.peer] < 0 ||
                e.message.sender != e.peer || e.message.epoch != kReplacementEpoch ||
                finished[e.peer])
                throw std::runtime_error("invalid handoff recovery event");
            const auto& m = e.message;
            auto& snapshot = snapshots[client_index[e.peer]];
            if (message_kind(m) == Kind::RecoveryDone) {
                if (m.value != snapshot.requests.size())
                    throw std::runtime_error("incomplete handoff snapshot");
                snapshot.issued_through = m.position;
                finished[e.peer] = true;
                ++count;
            } else if (message_kind(m) == Kind::RecoveryHandoffRequest &&
                       snapshot.requests.size() < 512) {
                snapshot.requests.push_back(m);
            } else throw std::runtime_error("invalid handoff snapshot record");
        }
        check_recovery_deadline(p.deadline);
    }
    auto promotion = index->reconcile(clients, snapshots, prepared_window);
    index.reset();
    install_relationships(net, clients, promotion.relationships, p.deadline);
    const auto reconciled = RecoveryClock::now();
    for (auto c : clients)
        net.send(c, message(Kind::RecoveryReady, replacement, 0, 0, kReplacementEpoch));
    const auto ready = RecoveryClock::now();
    print_recovery_result("HANDOFF_RECOVERY", replacement, p, reconciled, ready);
    recovery.recovered = p.length;
    recovery.promoted = true;
    run_handoff_leader(net, replacement, {replacement,other}, clients, locks,
        prepared_window, window, profile, 1, replication_batch, communication_batch,
        false, recovery, &promotion);
}
} // namespace lock
