#include "lock/locks/mu_state.h"
#include <cassert>
#include <stdexcept>
int main() {
    using namespace lock;
    MuState s(1000);
    auto a = make_request_id(4,1);
    auto b = make_request_id(5,1);
    auto acq_a = message(Kind::Acquire,4,3,a);
    auto acq_b = message(Kind::Acquire,5,3,b);
    auto first = s.apply(acq_a);
    assert(first.size() == 1 && first[0].destination == 4);
    assert(message_kind(first[0].message) == Kind::Grant);
    assert(s.apply(acq_b).empty());
    auto released = s.apply(message(Kind::Release,4,3,a));
    assert(released.size() == 2 && released[0].destination == 4 &&
           released[1].destination == 5);
    bool failed = false;
    try { s.apply(message(Kind::Release,4,3,a)); }
    catch (const std::runtime_error&) { failed = true; }
    assert(failed);
}
