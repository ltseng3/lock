#include "lock/locks/handoff_recovery.h"
#include <algorithm>
#include <limits>
#include <stdexcept>
#include <unordered_set>

namespace lock {
HandoffRecoveryIndex::HandoffRecoveryIndex(size_t locks, size_t max_slots)
    : counts_(locks), max_slots_(max_slots) {
    if (!locks || !max_slots || max_slots > std::numeric_limits<uint32_t>::max())
        throw std::invalid_argument("invalid handoff recovery index dimensions");
}
const HandoffRecoveryIndex::Entry* HandoffRecoveryIndex::find(RequestId id) const {
    const auto node = request_node(id);
    const auto seq = request_sequence(id);
    if (!node || node >= index_.size() || !seq)
        throw std::runtime_error("invalid handoff request identity");
    const auto& records = index_[node];
    if (seq >= records.size() || !records[seq].slot_plus_one) return nullptr;
    return &records[seq];
}
void HandoffRecoveryIndex::apply(const Message& m) {
    if (message_kind(m) != Kind::Acquire || m.sender != request_node(m.request) ||
        m.lock_id >= counts_.size() || applied_ >= max_slots_ || find(m.request))
        throw std::runtime_error("invalid or duplicate handoff log acquisition");
    const auto node = request_node(m.request);
    const auto seq = request_sequence(m.request);
    if (seq > max_slots_ + 512)
        throw std::runtime_error("handoff sequence exceeds recovery index bound");
    auto& records = index_[node];
    if (records.size() <= seq) records.resize(seq + 1);
    records[seq] = {applied_ + 1, m.lock_id, counts_[m.lock_id]++};
    highest_[node] = std::max(highest_[node], seq);
    ++applied_;
}
HandoffPromotion HandoffRecoveryIndex::reconcile(
    const std::vector<NodeId>& clients,
    const std::vector<HandoffClientSnapshot>& snapshots,
    size_t window) const {
    if (clients.empty() || snapshots.size() != clients.size())
        throw std::runtime_error("incomplete handoff membership");
    std::array<bool, 256> member{}, seen{};
    for (auto client : clients) {
        if (!client || client >= member.size() || member[client])
            throw std::runtime_error("invalid handoff membership");
        member[client] = true;
    }
    for (size_t n = 1; n < highest_.size(); ++n)
        if (highest_[n] && !member[n])
            throw std::runtime_error("log references excluded client");
    HandoffPromotion out{HandoffState(counts_.size(), window), {}, {}};
    struct Live { uint64_t slot; uint32_t ordinal; Message acquire; };
    std::vector<Live> live;
    std::vector<size_t> live_counts(counts_.size());
    std::unordered_set<RequestId> reported;
    for (const auto& snapshot : snapshots) {
        const auto n = snapshot.client;
        if (!n || n >= member.size() || !member[n] || seen[n] ||
            snapshot.issued_through < highest_[n] ||
            snapshot.issued_through > max_slots_ + 512 || snapshot.requests.size() > 512)
            throw std::runtime_error("invalid handoff client snapshot");
        seen[n] = true;
        for (const auto& m : snapshot.requests) {
            if (message_kind(m) != Kind::RecoveryHandoffRequest ||
                m.epoch != kReplacementEpoch || m.sender != n ||
                request_node(m.request) != n || request_sequence(m.request) == 0 ||
                request_sequence(m.request) > snapshot.issued_through ||
                m.lock_id >= counts_.size() || !reported.insert(m.request).second)
                throw std::runtime_error("invalid handoff recovery report");
            const auto phase = static_cast<HandoffReportPhase>(m.flags);
            const auto* entry = find(m.request);
            if (entry && entry->lock != m.lock_id)
                throw std::runtime_error("handoff recovery lock mismatch");
            if (phase == HandoffReportPhase::Ordered || phase == HandoffReportPhase::Finished) {
                if (!entry || m.value != entry->slot_plus_one - 1 || m.position != entry->ordinal)
                    throw std::runtime_error("handoff report disagrees with original order");
            } else if (phase != HandoffReportPhase::Pending || m.position || m.value) {
                throw std::runtime_error("invalid handoff report phase");
            }
            if (phase == HandoffReportPhase::Finished) continue;
            auto acquire = message(Kind::Acquire, n, m.lock_id, m.request, kReplacementEpoch);
            if (!entry) out.pending.push_back(acquire);
            else {
                live.push_back({entry->slot_plus_one - 1, entry->ordinal, acquire});
                ++live_counts[m.lock_id];
            }
        }
    }
    // All clients report all unfinished requests. An absent acquisition
    // has completed, but an unfinished predecessor cannot precede a
    // completed successor. A gap is a protocol inconsistency, not permission
    // to guess a holder. This check also bounds the reconstructed queue.
    std::vector<uint64_t> next(counts_.size());
    for (size_t l = 0; l < counts_.size(); ++l) {
        if (live_counts[l] > window || live_counts[l] > counts_[l])
            throw std::runtime_error("recovered prepared window exceeded");
        next[l] = counts_[l] - live_counts[l];
    }
    std::sort(live.begin(), live.end(), [](const Live& a, const Live& b) {
        return a.slot < b.slot;
    });
    for (const auto& item : live) {
        if (item.ordinal != next[item.acquire.lock_id]++)
            throw std::runtime_error("unfinished handoff acquisitions are not a suffix");
        // Reuse the normal reservation/commit state machine; these headers
        // are already committed, so do not append them to the physical log.
        auto step = out.state.admit(item.acquire);
        if (step.append.size() != 1 || !step.actions.empty())
            throw std::runtime_error("cannot restore handoff reservation");
        auto actions = out.state.committed({item.slot, item.acquire});
        out.relationships.insert(out.relationships.end(), actions.begin(), actions.end());
    }
    // Pending unlogged acquisitions have no committed cross-client order.
    // Deterministic order for testing, not an inferred historical order.
    std::sort(out.pending.begin(), out.pending.end(), [](const Message& a, const Message& b) {
        return a.request < b.request;
    });
    return out;
}
} // namespace lock
