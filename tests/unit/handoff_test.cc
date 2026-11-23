#include "lock/locks/handoff_state.h"
#include "lock/locks/handoff_layout.h"
#include <stdexcept>
#include <cassert>
#include <map>
int main() {
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
    markers[0] = a;
    auto progress = state.retire([&](uint64_t slot) { return markers[slot]; });
    assert(progress.actions.size() == 2 && progress.append.size() == 1);
    assert(progress.append[0].request == c);
    x = state.committed({2, mc});
    assert(x.size() == 1 && message_kind(x[0].message) == Kind::Order &&
           x[0].message.other == 0);
    assert(x[0].message.position == 2);
}
