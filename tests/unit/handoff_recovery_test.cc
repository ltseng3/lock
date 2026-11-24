#include "lock/locks/handoff_recovery.h"
#include "lock/locks/handoff_layout.h"
#include <stdexcept>
#include <algorithm>
#include <array>
#include <random>
#include <unordered_map>
using namespace lock;
void check(bool ok) { if (!ok) throw std::runtime_error("handoff recovery test"); }
template<class F> void rejects(F f) {
    bool failed = false;
    try { f(); } catch (const std::exception&) { failed = true; }
    check(failed);
}
Message acquire(NodeId n, uint64_t seq, LockId l = 0) {
    return message(Kind::Acquire,n,l,make_request_id(n,seq));
}
Message report(Message a, HandoffReportPhase phase, uint64_t slot = 0, uint64_t pos = 0) {
    a.kind = static_cast<uint32_t>(Kind::RecoveryHandoffRequest);
    a.epoch = kReplacementEpoch;
    a.flags = static_cast<uint32_t>(phase);
    a.value = slot; a.position = pos;
    return a;
}
// Generate immutable histories over multiple locks and clients, then
// independently choose a valid completion frontier on every lock.
void test_random_frontiers() {
    std::mt19937 rng(2026);
    constexpr size_t locks = 5, clients = 3, history = 90, window = 8;
    for (size_t trial = 0; trial < 200; ++trial) {
        HandoffRecoveryIndex index(locks, history + 32);
        struct Item { Message acquire; uint64_t slot, ordinal; };
        std::array<std::vector<Item>,locks> by_lock;
        std::array<uint64_t,clients> seq{};
        for (size_t i = 0; i < history; ++i) {
            const size_t node = rng() % clients, l = rng() % locks;
            auto a = acquire(static_cast<NodeId>(node + 4), ++seq[node], l);
            by_lock[l].push_back({a,i,by_lock[l].size()});
            index.apply(a);
        }
        std::vector<HandoffClientSnapshot> snapshots;
        for (size_t c = 0; c < clients; ++c)
            snapshots.push_back({static_cast<NodeId>(c+4),seq[c],{}});
        std::unordered_map<RequestId,RequestId> expected_predecessor;
        std::unordered_map<RequestId,uint64_t> expected_slot;
        size_t live_count = 0;
        for (const auto& entries : by_lock) {
            const size_t live = rng() % (std::min(window,entries.size())+1);
            RequestId prev = 0;
            for (size_t i = entries.size()-live; i < entries.size(); ++i) {
                const auto& item = entries[i];
                auto& s = snapshots[request_node(item.acquire.request)-4];
                s.requests.push_back((rng() & 1) ?
                    report(item.acquire,HandoffReportPhase::Ordered,item.slot,item.ordinal) :
                    report(item.acquire,HandoffReportPhase::Pending));
                expected_predecessor[item.acquire.request] = prev;
                expected_slot[item.acquire.request] = item.slot;
                prev = item.acquire.request;
                ++live_count;
            }
        }
        auto out = index.reconcile({4,5,6},snapshots,window);
        check(out.pending.empty());
        size_t orders = 0;
        for (const auto& action : out.relationships) {
            const auto& m = action.message;
            if (message_kind(m) == Kind::Order) {
                check(m.other == expected_predecessor.at(m.request));
                check(m.value == expected_slot.at(m.request));
                ++orders;
            } else {
                check(message_kind(m) == Kind::Next);
                check(expected_predecessor.at(m.other) == m.request);
            }
        }
        check(orders == live_count);
        // Every restored slot can retire using normal marker processing.
        std::unordered_map<uint64_t,RequestId> markers;
        for (const auto& [request,slot] : expected_slot) markers[slot] = request;
        auto retired = out.state.retire([&](uint64_t slot) { return markers.at(slot); });
        check(retired.actions.size() == live_count && retired.append.empty());
        check(out.state.retire([&](uint64_t slot) { return markers.at(slot); }).actions.empty());
    }
}
int main() {
    test_random_frontiers();
    HandoffRecoveryIndex index(2, 100);
    auto a = acquire(4,1), b = acquire(5,1), c = acquire(4,2);
    auto d = acquire(5,2,1), e = acquire(5,3);
    index.apply(a); index.apply(b); index.apply(d); index.apply(c);
    check(index.applied() == 4);
    rejects([&] { index.apply(a); });
    // A completed, B ordered, C committed but ORDER lost, D finished
    // with deferred marker. E never logged.
    HandoffClientSnapshot s4{4,2,{report(c,HandoffReportPhase::Pending)}};
    HandoffClientSnapshot s5{5,3,{report(b,HandoffReportPhase::Ordered,1,1),
        report(d,HandoffReportPhase::Finished,2,0),report(e,HandoffReportPhase::Pending)}};
    auto result = index.reconcile({4,5},{s4,s5},4);
    check(result.pending.size() == 1 && result.pending[0].request == e.request);
    check(result.relationships.size() == 3);
    check(result.relationships[0].message.request == b.request &&
          result.relationships[0].message.other == 0 &&
          result.relationships[0].message.value == 1);
    check(result.relationships[1].message.request == c.request &&
          result.relationships[1].message.other == b.request &&
          result.relationships[1].message.value == 3);
    check(message_kind(result.relationships[2].message) == Kind::Next);
    auto retired = result.state.retire([&](uint64_t slot) { return slot == 1 ? b.request : 0; });
    check(retired.actions.size() == 1 && retired.actions[0].message.request == b.request);
    rejects([&] { index.reconcile({4,5},{s4},4); });
    rejects([&] { index.reconcile({4,5},{s4,s4},4); });
    rejects([&] { index.reconcile({4,5},{s4,s5},1); });
    auto bad = s5; bad.requests[0].value = 0;
    rejects([&] { index.reconcile({4,5},{s4,bad},4); });
    // An unfinished earlier predecessor cannot have a completed successor.
    auto missing = s4; missing.requests.clear();
    rejects([&] { index.reconcile({4,5},{missing,s5},4); });
    bad = s5; bad.issued_through = 1;
    rejects([&] { index.reconcile({4,5},{s4,bad},4); });
    const size_t memory = 512ULL*1024*1024;
    const auto capacity = handoff_log_capacity(memory,true);
    check(handoff_marker_offset(memory,0,true) == rdma::kLogOffset + capacity*8);
    check(handoff_marker_offset(memory,capacity-1,true)+8 ==
          compact_publication_offset(memory,0,true));
    check(compact_publication_offset(memory,capacity-1,true) < epoch_boundary_base(memory));
}
