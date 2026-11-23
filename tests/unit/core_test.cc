#include "lock/config.h"
#include "lock/types.h"
#include <cassert>
#include <stdexcept>
int main() {
    using namespace lock;
    auto r = make_request_id(8, 42);
    assert(request_node(r) == 8 && request_sequence(r) == 42);
    assert(message_kind(message(Kind::Handoff, 7, 999, r)) == Kind::Handoff);
    bool invalid = false;
    try { make_request_id(0, 1); } catch (const std::invalid_argument&) { invalid = true; }
    assert(invalid);
    auto bad = message(Kind::Acquire, 1);
    bad.version = 0;
    invalid = false;
    try { (void)message_kind(bad); } catch (const std::runtime_error&) { invalid = true; }
    assert(invalid);
    char a0[] = "lock"; char a1[] = "--replicas=3"; char a2[] = "--nodes=1,2,3";
    char* argv[] = {a0,a1,a2};
    Options opts(3, argv);
    assert(opts.number("replicas", 0) == 3);
    assert(opts.list("nodes").size() == 3);
}
