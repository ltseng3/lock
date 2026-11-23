#pragma once
#include "lock/locks/mu_state.h"
#include "lock/locks/replicated_log.h"
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <vector>
namespace lock {
struct HandoffStep {
    std::vector<Action> actions;
    std::vector<Message> append;
};
class HandoffState {
public:
    HandoffState(size_t locks, size_t prepared_window);
    HandoffStep admit(const Message& acquire);
    std::vector<Action> committed(const Committed& item);
    HandoffStep retire(const std::function<uint64_t(uint64_t)>& read_marker);
private:
    struct Entry { RequestId request; uint64_t position; uint64_t log_slot; };
    struct State {
        uint64_t next_position = 0;
        size_t uncommitted = 0;
        std::deque<Entry> prepared;
        std::deque<Message> waiting;
    };
    size_t window_;
    std::vector<State> locks_;
    // Only locks with committed, not-yet-retired entries need marker polling.
    // The replication thread exclusively owns this list and every State.
    std::vector<size_t> active_locks_;
    void admit_waiting(State& state, HandoffStep& out);
};
} // namespace lock
