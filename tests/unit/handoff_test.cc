#include "lock/locks/handoff_state.h"
#include "lock/locks/handoff_layout.h"
#include <stdexcept>
#include <cassert>
#include <map>
namespace {
using namespace lock;
void require(bool value) {
    if (!value) throw std::runtime_error("handoff recovery regression");
}
void test_fifo_recovery() {
    using namespace std::chrono_literals;
    const auto start = LeaseClock::time_point{};
    HandoffState state(4, 3, false, 10000);
    const auto a = make_request_id(4, 1), b = make_request_id(5, 1);
    const auto c = make_request_id(6, 1), d = make_request_id(7, 1);
    const auto failed_waiter = make_request_id(5, 2), newer = make_request_id(8, 1);
    const auto acquire = [](RequestId r) { return message(Kind::Acquire, request_node(r), 1, r); };
    for (auto r : {a,b,c}) require(state.admit(acquire(r)).append.size() == 1);
    require(state.committed({0, acquire(a)}, start).size() == 1);
    require(state.committed({1, acquire(b)}, start + 1ms).size() == 2);
    // c is reserved but not committed when b is excluded.
    require(state.admit(acquire(d)).append.empty());
    require(state.admit(acquire(failed_waiter)).append.empty());
    state.exclude_client(5, start + 2ms);
    state.exclude_client(5, start + 2ms);
    require(state.recovering());
    require(state.recovery_stats().failed_waiting == 1);
    require(state.committed({2, acquire(c)}, start + 3ms).empty());
    // No further authority until the whole issued suffix is safe.
    std::map<uint64_t,uint64_t> markers{{0,a}};
    auto read = [&](uint64_t slot) { return markers[slot]; };
    require(state.retire(read).actions.size() == 1);
    require(state.recover(start + 10ms, read).actions.empty());
    auto queries = state.recover(start + 11ms, read);
    require(queries.actions.size() == 2 && queries.append.empty());
    require(queries.actions[0].message.request == c && queries.actions[1].message.request == d);
    require(message_kind(queries.actions[0].message) == Kind::HandoffReconcile);
    require(state.admit(acquire(newer)).append.empty());
    auto fresh_c = make_request_id(6, 2), fresh_d = make_request_id(7, 2);
    auto reply = message(Kind::HandoffReconciled, 7, 1, d);
    reply.other = fresh_d;
    state.reconciled(reply); // replies may arrive in reverse order
    require(state.recover(start + 12ms, read).append.empty());
    reply = message(Kind::HandoffReconciled, 6, 1, c);
    reply.other = fresh_c;
    state.reconciled(reply);
    auto repaired = state.recover(start + 13ms, read);
    require(!state.recovering() && repaired.append.size() == 3);
    require(repaired.append[0].request == fresh_c && repaired.append[1].request == fresh_d &&
            repaired.append[2].request == newer);
    auto actions = state.committed({3, repaired.append[0]}, start + 13ms);
    require(actions.size() == 1 && actions[0].message.other == 0);
    require(actions[0].message.position == 3);
    actions = state.committed({4, repaired.append[1]}, start + 13ms);
    require(actions.size() == 2 && actions[0].message.other == fresh_c);
    // The old marker slot is never reused.
    markers[2] = c;
    require(state.retire(read).actions.empty());
}
void test_missing_marker_suffix() {
    using namespace std::chrono_literals;
    const auto start = LeaseClock::time_point{};
    HandoffState state(2, 4, false, 10000);
    const auto a = make_request_id(4, 1), b = make_request_id(5, 1);
    const auto c = make_request_id(6, 1);
    for (auto r : {a,b,c}) {
        auto m = message(Kind::Acquire, request_node(r), 1, r);
        require(state.admit(m).append.size() == 1);
        state.committed({request_node(r), m}, start);
    }
    // A failed before its marker; B could have executed already.
    state.exclude_client(4, start + 1ms);
    auto read = [](uint64_t) { return uint64_t{0}; };
    require(state.recover(start + 9ms, read).actions.empty());
    auto queries = state.recover(start + 10ms, read);
    require(queries.actions.size() == 2);
    state.reconciled(message(Kind::HandoffReconciled, 5, 1, b)); // B executed
    auto reply = message(Kind::HandoffReconciled, 6, 1, c);
    reply.other = make_request_id(6, 2);
    state.reconciled(reply);
    auto repaired = state.recover(start + 11ms, read);
    require(repaired.append.size() == 1 && repaired.append[0].request == reply.other);
    require(state.recovery_stats().executed == 1);
}
}
int main() {
    test_fifo_recovery();
    test_missing_marker_suffix();
    using namespace lock;
    constexpr size_t mr_bytes = 512ULL * 1024 * 1024;
    const size_t capacity = handoff_log_capacity(mr_bytes);
    assert(capacity > 1000000);
    assert(handoff_header_offset(mr_bytes, 0) == rdma::kLogOffset);
    assert(handoff_marker_offset(mr_bytes, 0) ==
           rdma::kLogOffset + capacity * sizeof(uint64_t));
    assert(handoff_header_offset(mr_bytes, capacity - 1) + 8 <=
           handoff_marker_offset(mr_bytes, 0));
    assert(handoff_marker_offset(mr_bytes, capacity - 1) + 8 <=
           mr_bytes - kHandoffScratchReserve);
    bool rejected = false;
    try { (void)handoff_marker_offset(mr_bytes, capacity); }
    catch (const std::out_of_range&) { rejected = true; }
    assert(rejected);

    const auto t = LeaseClock::time_point{};
    ProgressWatch watch;
    if (!watch.observe(1, t) || watch.suspect(t + std::chrono::microseconds(999), 1000) ||
        !watch.suspect(t + std::chrono::microseconds(1000), 1000) ||
        watch.suspect(t + std::chrono::microseconds(2000), 1000) ||
        !watch.observe(2, t + std::chrono::microseconds(2000)) ||
        watch.suspect(t + std::chrono::microseconds(2001), 1000))
        throw std::runtime_error("progress watch regression");
    if (heartbeat_offset(1) + 64 != heartbeat_offset(2) ||
        lease_elapsed(t, t + std::chrono::microseconds(999), 1000) ||
        !lease_elapsed(t, t + std::chrono::microseconds(1000), 1000))
        throw std::runtime_error("lease deadline/layout regression");
    HandoffState state(1000, 2);
    bool bad_lock = false;
    try { (void)state.retire_lock(1000, [](uint64_t) { return uint64_t{0}; }); }
    catch (const std::out_of_range&) { bad_lock = true; }
    assert(bad_lock);
    auto a = make_request_id(4, 1), b = make_request_id(5, 1), c = make_request_id(6, 1);
    auto ma = message(Kind::Acquire, 4, 17, a);
    auto mb = message(Kind::Acquire, 5, 17, b);
    auto mc = message(Kind::Acquire, 6, 17, c);
    assert(state.admit(ma).append.size() == 1);
    auto x = state.committed({0, ma});
    assert(x.size() == 1 && message_kind(x[0].message) == Kind::Order &&
           x[0].message.other == 0);
    assert(state.admit(mb).append.size() == 1);
    x = state.committed({1, mb});
    assert(x.size() == 2 && message_kind(x[1].message) == Kind::Next);
    assert(x[1].message.request == a && x[1].message.other == b);
    assert(state.admit(mc).append.empty());
    std::map<uint64_t,uint64_t> markers{{1,b}};
    auto blocked = state.retire([&](uint64_t slot) { return markers[slot]; });
    assert(blocked.actions.empty());
    LeaseScanStats stats;
    state.monitor_heads(t, 1000, stats);
    state.monitor_heads(t + std::chrono::milliseconds(1), 1000, stats);
    state.monitor_heads(t + std::chrono::milliseconds(2), 1000, stats);
    if (stats.suspicions != 1 || stats.checks != 3)
        throw std::runtime_error("head monitor changed or skipped unresolved head");
    markers[0] = a;
    auto progress = state.retire([&](uint64_t slot) { return markers[slot]; });
    assert(progress.actions.size() == 2 && progress.append.size() == 1);
    assert(progress.append[0].request == c);
    x = state.committed({2, mc});
    assert(x.size() == 1 && message_kind(x[0].message) == Kind::Order &&
           x[0].message.other == 0);
    assert(x[0].message.position == 2);
}
