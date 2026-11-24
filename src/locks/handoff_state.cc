#include "lock/locks/handoff_state.h"
#include <stdexcept>
namespace lock {
HandoffState::HandoffState(size_t locks, size_t window, bool gc)
    : window_(window), gc_(gc), locks_(locks) {
    if (!locks || !window) throw std::invalid_argument("empty handoff locks or window");
}
void HandoffState::admit_waiting(State& state, HandoffStep& out) {
    while (!state.waiting.empty() &&
           state.prepared.size() + state.uncommitted < window_) {
        Message m = state.waiting.front();
        state.waiting.pop_front();
        ++state.uncommitted;
        out.append.push_back(m);
    }
}
HandoffStep HandoffState::admit(const Message& acquire) {
    if (message_kind(acquire) != Kind::Acquire || acquire.request == 0 ||
        acquire.lock_id >= locks_.size())
        throw std::runtime_error("invalid handoff acquire");
    auto& state = locks_[acquire.lock_id];
    state.waiting.push_back(acquire);
    HandoffStep out;
    admit_waiting(state, out);
    return out;
}
std::vector<Action> HandoffState::committed(const Committed& item) {
    const auto& command = item.command;
    if (message_kind(command) != Kind::Acquire || command.lock_id >= locks_.size())
        throw std::runtime_error("unexpected handoff commit");
    auto& state = locks_[command.lock_id];
    if (!state.uncommitted || state.prepared.size() >= window_)
        throw std::runtime_error("handoff reservation invariant violated");
    --state.uncommitted;
    RequestId predecessor = state.prepared.empty() ? 0 : state.prepared.back().request;
    uint64_t predecessor_position = state.prepared.empty() ? 0 :
                                    state.prepared.back().position;
    uint64_t position = state.next_position++;
    state.prepared.push_back({command.request, position, item.global_slot});
    if (state.prepared.size() == 1) {
        if (state.active_index != static_cast<size_t>(-1))
            throw std::runtime_error("duplicate active handoff lock");
        state.active_index = active_locks_.size();
        active_locks_.push_back(command.lock_id);
    }
    Message order = message(Kind::Order, 0, command.lock_id, command.request);
    order.other = predecessor;
    order.position = position;
    order.value = item.global_slot;
    std::vector<Action> result{{request_node(command.request), order}};
    if (predecessor != 0) {
        // ORDER with no predecessor already conveys a committed, immediately
        // active ownership slot; no separate START RPC is necessary.
        // When there is a predecessor, notify it about the next owner.
        Message next = message(Kind::Next, 0, command.lock_id, predecessor);
        next.other = command.request;
        next.position = predecessor_position;
        next.value = position;
        result.push_back({request_node(predecessor), next});
    }
    return result;
}
void HandoffState::retire_one(
    size_t lock_id,
    const std::function<uint64_t(uint64_t)>& read_marker,
    HandoffStep& out) {
    if (lock_id >= locks_.size())
        throw std::out_of_range("handoff retirement lock ID out of range");
    auto& state = locks_[lock_id];
    if (state.active_index == static_cast<size_t>(-1)) return;
    if (state.active_index >= active_locks_.size() ||
        active_locks_[state.active_index] != lock_id || state.prepared.empty())
        throw std::runtime_error("invalid active handoff lock index");
    while (!state.prepared.empty()) {
        const auto& head = state.prepared.front();
        const uint64_t marker = read_marker(head.log_slot);
        if (!marker) break;
        if (marker != head.request)
            throw std::runtime_error("release-marker request mismatch: fail closed");
        // A committed successor already caused a unique NEXT to be queued.
        // Only the last prepared owner needs explicit no-successor proof.
        if (!gc_ || state.prepared.size() == 1) {
            Message ack = message(Kind::ReleaseAck, 0, lock_id, head.request);
            ack.position = head.position;
            out.actions.push_back({request_node(head.request), ack});
        }
        state.prepared.pop_front();
    }
    admit_waiting(state, out);
    if (state.prepared.empty()) {
        // Swap-remove rather than shifting a potentially large active list.
        const size_t index = state.active_index;
        const size_t moved = active_locks_.back();
        active_locks_[index] = moved;
        locks_[moved].active_index = index;
        active_locks_.pop_back();
        state.active_index = static_cast<size_t>(-1);
    }
}
HandoffStep HandoffState::retire_lock(
    LockId lock_id,
    const std::function<uint64_t(uint64_t)>& read_marker) {
    HandoffStep out;
    retire_one(lock_id, read_marker, out);
    return out;
}
HandoffStep HandoffState::retire(
    const std::function<uint64_t(uint64_t)>& read_marker) {
    HandoffStep out;
    size_t i = 0;
    while (i < active_locks_.size()) {
        const size_t current = active_locks_[i];
        retire_one(current, read_marker, out);
        // Retired entries swap the last active lock into the current slot.
        // Revisit that slot to avoid skipping a holder.
        if (i < active_locks_.size() && active_locks_[i] == current)
            ++i;
    }
    return out;
}
} // namespace lock
