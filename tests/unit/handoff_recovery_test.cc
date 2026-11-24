#include "lock/locks/handoff_recovery.h"
#include "lock/locks/handoff_layout.h"
#include <stdexcept>
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
int main() {
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
