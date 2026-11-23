#include "lock/locks/handoff_state.h"
#include <cassert>
#include <cstdint>
#include <deque>
#include <map>
#include <stdexcept>
#include <vector>

int main() {
    using namespace lock;
    constexpr size_t kRequests = 1000;
    constexpr size_t kWindow = 8;
    HandoffState state(1, kWindow);
    std::vector<RequestId> requests;
    requests.reserve(kRequests);
    std::deque<Message> ready;
    for (size_t i = 0; i < kRequests; ++i) {
        NodeId node = 4 + i % 5;
        RequestId id = make_request_id(node, i + 1);
        requests.push_back(id);
        auto step = state.admit(message(Kind::Acquire, node, 0, id));
        for (auto& m : step.append) ready.push_back(m);
    }
    assert(ready.size() == kWindow);

    std::map<uint64_t,uint64_t> markers;
    size_t committed = 0, acknowledged = 0;
    while (acknowledged < kRequests) {
        while (!ready.empty()) {
            Message m = ready.front();
            ready.pop_front();
            auto actions = state.committed({committed, m});
            assert(actions.size() == 1 || actions.size() == 2);
            assert(actions[0].message.position == committed);
            assert(actions[0].message.value == committed);
            assert(actions[0].message.request == requests[committed]);
            // An ORDER without a predecessor immediately activates ownership;
            // otherwise NEXT notifies the already-ordered predecessor.
            if (actions.size() == 1) {
                assert(actions[0].message.other == 0);
            } else {
                assert(message_kind(actions[1].message) == Kind::Next);
                assert(actions[1].message.other == m.request);
                assert(actions[1].message.position + 1 == committed);
            }
            ++committed;
        }
        assert(committed > acknowledged);
        // Install evidence for the next position first. The leader must not
        // skip a still-unreleased head even if it sees a later marker.
        if (committed >= acknowledged + 2)
            markers[acknowledged + 1] = requests[acknowledged + 1];
        auto blocked = state.retire([&](uint64_t s) { return markers[s]; });
        assert(blocked.actions.empty());
        assert(blocked.append.empty());
        markers[acknowledged] = requests[acknowledged];
        auto progress = state.retire([&](uint64_t s) { return markers[s]; });
        assert(!progress.actions.empty());
        for (auto& action : progress.actions) {
            assert(message_kind(action.message) == Kind::ReleaseAck);
            assert(action.message.request == requests[acknowledged]);
            ++acknowledged;
        }
        for (auto& m : progress.append) ready.push_back(m);
        assert(ready.size() <= kWindow);
    }
    assert(committed == kRequests);

    // With many configured but idle locks, only a committed active lock may
    // trigger retirement-marker polling.
    HandoffState sparse(1000, 4);
    auto sparse_id = make_request_id(4, 321);
    auto sparse_msg = message(Kind::Acquire, 4, 997, sparse_id);
    assert(sparse.admit(sparse_msg).append.size() == 1);
    int probes = 0;
    auto no_marker = [&](uint64_t) { ++probes; return uint64_t{0}; };
    assert(sparse.retire(no_marker).actions.empty());
    assert(probes == 0);
    sparse.committed({0, sparse_msg});
    assert(sparse.retire(no_marker).actions.empty());
    assert(probes == 1);
    auto done = sparse.retire([&](uint64_t) {
        ++probes;
        return sparse_id;
    });
    assert(done.actions.size() == 1);
    assert(sparse.retire(no_marker).actions.empty());
    assert(probes == 2); // The lock was removed from the active list.

    // When a committed holder is already retired, process its visible marker
    // BEFORE committing the next prepared acquisition. The new ORDER must
    // have predecessor=0 and be immediately active without NEXT/HANDOFF.
    HandoffState released_first(4, 4);
    auto first_id = make_request_id(4, 9001);
    auto second_id = make_request_id(5, 9001);
    auto first_req = message(Kind::Acquire, 4, 1, first_id);
    auto second_req = message(Kind::Acquire, 5, 1, second_id);
    assert(released_first.admit(first_req).append.size() == 1);
    auto first_order = released_first.committed({0, first_req});
    assert(first_order.size() == 1 && first_order[0].message.other == 0);
    assert(released_first.admit(second_req).append.size() == 1);
    auto released = released_first.retire([&](uint64_t slot) {
        assert(slot == 0);
        return first_id;
    });
    assert(released.actions.size() == 1);
    assert(released.actions[0].message.request == first_id);
    auto second_order = released_first.committed({1, second_req});
    assert(second_order.size() == 1 && second_order[0].message.other == 0);
    // When the marker is NOT visible, the successor still waits for
    // predecessor-triggered handoff.
    HandoffState occupied(4, 4);
    assert(occupied.admit(first_req).append.size() == 1);
    assert(occupied.committed({0, first_req}).size() == 1);
    assert(occupied.admit(second_req).append.size() == 1);
    auto waiting = occupied.committed({1, second_req});
    assert(waiting.size() == 2 &&
           waiting[0].message.other == first_id &&
           message_kind(waiting[1].message) == Kind::Next);

    HandoffState corrupted(1,1);
    auto id = make_request_id(4,1);
    auto entry = message(Kind::Acquire,4,0,id);
    assert(corrupted.admit(entry).append.size() == 1);
    corrupted.committed({0,entry});
    bool refused = false;
    try {
        corrupted.retire([&](uint64_t) { return make_request_id(5,1); });
    } catch (const std::runtime_error&) { refused = true; }
    assert(refused); // Foreign marker never grants a successor.
}
