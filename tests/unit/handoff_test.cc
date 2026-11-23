#include "lock/locks/handoff_state.h"
#include <cassert>
#include <map>
int main() {
    using namespace lock;
    HandoffState state(1000, 2);
    auto a = make_request_id(4, 1), b = make_request_id(5, 1), c = make_request_id(6, 1);
    auto ma = message(Kind::Acquire, 4, 17, a);
    auto mb = message(Kind::Acquire, 5, 17, b);
    auto mc = message(Kind::Acquire, 6, 17, c);
    assert(state.admit(ma).append.size() == 1);
    auto x = state.committed({0, ma});
    assert(x.size() == 2 && message_kind(x[1].message) == Kind::Start);
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
    assert(x.size() == 2 && message_kind(x[1].message) == Kind::Start);
    assert(x[0].message.position == 2);
}
