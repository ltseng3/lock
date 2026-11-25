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
    HandoffState(size_t locks, size_t prepared_window, bool gc = false);
    HandoffStep admit(const Message& acquire);
    std::vector<Action> committed(const Committed& item);
    HandoffStep retire(const std::function<uint64_t(uint64_t)>& read_marker);
    // Check just the committed holder(s) of this lock, particularly before
    // assigning a predecessor to a newly committed request on the same lock.
    HandoffStep retire_lock(LockId lock_id,
                            const std::function<uint64_t(uint64_t)>& read_marker);
    // Read-only diagnostic snapshot; called only by the owning replication thread.
    struct Snapshot {
        uint64_t waiting, prepared, uncommitted, next_position, head_request;
        uint64_t head_marker, later_markers;
    };
    Snapshot snapshot(size_t id,
                      const std::function<uint64_t(uint64_t)>& read_marker) const {
        const auto& s = locks_.at(id);
        Snapshot out{s.waiting.size(), s.prepared.size(), s.uncommitted,
                     s.next_position, 0, 0, 0};
        if (!s.prepared.empty()) {
            out.head_request = s.prepared.front().request;
            out.head_marker = read_marker(s.prepared.front().log_slot);
            if (!out.head_marker)
                for (size_t i = 1; i < s.prepared.size(); ++i)
                    if (read_marker(s.prepared[i].log_slot)) ++out.later_markers;
        }
        return out;
    }
private:
    struct Entry { RequestId request; uint64_t position; uint64_t log_slot; };
    struct State {
        uint64_t next_position = 0;
        size_t uncommitted = 0;
        std::deque<Entry> prepared;
        std::deque<Message> waiting;
        // O(1) removal from the list of locks that have a committed holder.
        size_t active_index = static_cast<size_t>(-1);
    };
    size_t window_;
    bool gc_;
    std::vector<State> locks_;
    // Only locks with committed, not-yet-retired entries need marker polling.
    // The replication thread exclusively owns this list and every State.
    std::vector<size_t> active_locks_;
    void admit_waiting(State& state, HandoffStep& out);
    void retire_one(size_t lock_id,
                    const std::function<uint64_t(uint64_t)>& read_marker,
                    HandoffStep& out);
};
} // namespace lock
