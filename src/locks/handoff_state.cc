#include "lock/locks/handoff_state.h"
#include <stdexcept>
namespace lock {
HandoffState::HandoffState(size_t locks, size_t window, bool gc, uint64_t authority_us)
    : window_(window), gc_(gc), authority_us_(authority_us), locks_(locks) {
    if (!locks || !window) throw std::invalid_argument("empty handoff locks or window");
    if (authority_us && gc) throw std::invalid_argument("recovery does not support GC");
}
bool HandoffState::excluded(RequestId request) const {
    const auto node = request_node(request);
    if (!node || node >= excluded_.size())
        throw std::runtime_error("invalid handoff client ID");
    return excluded_[node];
}
void HandoffState::admit_waiting(State& state, HandoffStep& out) {
    if (state.repair != Repair::Normal) return;
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
    if (authority_us_ && excluded(acquire.request)) {
        ++recovery_stats_.failed_waiting;
        return {};
    }
    auto& state = locks_[acquire.lock_id];
    state.waiting.push_back(acquire);
    HandoffStep out;
    admit_waiting(state, out);
    return out;
}
std::vector<Action> HandoffState::committed(const Committed& item, LeaseClock::time_point now) {
    const auto& command = item.command;
    if (message_kind(command) != Kind::Acquire || command.lock_id >= locks_.size())
        throw std::runtime_error("unexpected handoff commit");
    auto& state = locks_[command.lock_id];
    if (!state.uncommitted || state.prepared.size() >= window_)
        throw std::runtime_error("handoff reservation invariant violated");
    --state.uncommitted;
    if (authority_us_ && excluded(command.request))
        begin_repair(command.lock_id, now);
    RequestId predecessor = state.prepared.empty() ? 0 : state.prepared.back().request;
    uint64_t predecessor_position = state.prepared.empty() ? 0 :
                                    state.prepared.back().position;
    uint64_t position = state.next_position++;
    // An ORDER/NEXT already queued may still arrive. Only suppress authority
    // for commitments applied after the freeze. Never roll back the log.
    const bool issue = state.repair == Repair::Normal;
    const auto expires = issue && authority_us_
        ? now + std::chrono::microseconds(authority_us_) : now;
    state.prepared.push_back({command.request, position, item.global_slot, expires});
    if (state.prepared.size() == 1) {
        if (state.active_index != static_cast<size_t>(-1))
            throw std::runtime_error("duplicate active handoff lock");
        state.active_index = active_locks_.size();
        active_locks_.push_back(command.lock_id);
    }
    if (!issue) return {};
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
    if (state.active_index == static_cast<size_t>(-1) ||
        state.repair == Repair::Reconciling) return;
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
    if (state.prepared.empty()) remove_active(state);
}
void HandoffState::remove_active(State& state) {
    if (state.active_index == static_cast<size_t>(-1)) return;
    const size_t index = state.active_index;
    const size_t moved = active_locks_.back();
    active_locks_[index] = moved;
    locks_[moved].active_index = index;
    active_locks_.pop_back();
    state.active_index = static_cast<size_t>(-1);
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
void HandoffState::monitor_heads(LeaseClock::time_point now, uint64_t timeout_us,
                                 LeaseScanStats& stats) {
    if (!timeout_us) throw std::invalid_argument("zero lease monitor timeout");
    if (lease_heads_.empty()) lease_heads_.resize(locks_.size());
    ++stats.scans;
    for (auto lock : active_locks_)
        stats.sample(lease_heads_[lock], locks_[lock].prepared.front().request, now, timeout_us);
}

void HandoffState::begin_repair(size_t lock_id, LeaseClock::time_point now) {
    auto& state = locks_.at(lock_id);
    if (state.repair != Repair::Normal) return;
    state.repair = Repair::Draining;
    state.repair_started = now;
    recovering_locks_.push_back(lock_id);
    ++recovery_stats_.started;
}

void HandoffState::exclude_client(NodeId peer, LeaseClock::time_point now) {
    if (!authority_us_ || !peer || peer >= excluded_.size())
        throw std::runtime_error("invalid client exclusion");
    if (excluded_[peer]) return;
    if (std::any_of(excluded_.begin(), excluded_.end(), [](bool value) { return value; }))
        throw std::runtime_error("only one client exclusion is supported");
    excluded_[peer] = true;
    for (size_t lock_id = 0; lock_id < locks_.size(); ++lock_id) {
        auto& state = locks_[lock_id];
        recovery_stats_.failed_waiting += std::erase_if(state.waiting, [peer](const Message& m) {
            return request_node(m.request) == peer;
        });
        if (std::any_of(state.prepared.begin(), state.prepared.end(), [peer](const Entry& e) {
                return request_node(e.request) == peer;
            }))
            begin_repair(lock_id, now);
        // A failed client may have been asked to reconcile before detection.
        // No reply is needed once its old authority was proved safe.
        for (auto& entry : state.reconcile) {
            if (request_node(entry.old) == peer) {
                entry.answered = true;
                entry.fresh = 0;
            }
        }
    }
}

void HandoffState::expired(const Message& notice, LeaseClock::time_point now) {
    if (!authority_us_ || message_kind(notice) != Kind::HandoffExpired ||
        notice.lock_id >= locks_.size() || !notice.request ||
        request_node(notice.request) != notice.sender)
        throw std::runtime_error("invalid handoff expiry notice");
    auto& state = locks_[notice.lock_id];
    if (std::any_of(state.prepared.begin(), state.prepared.end(), [&](const Entry& e) {
            return e.request == notice.request;
        })) {
        ++recovery_stats_.expired_notices;
        begin_repair(notice.lock_id, now);
    }
}

void HandoffState::expire_heads(LeaseClock::time_point now) {
    if (!authority_us_) return;
    for (auto lock_id : active_locks_) {
        auto& state = locks_[lock_id];
        if (state.repair == Repair::Normal && now >= state.prepared.front().expires) {
            ++recovery_stats_.expired_heads;
            begin_repair(lock_id, now);
        }
    }
}

void HandoffState::query_survivors(
    size_t lock_id, State& state, HandoffStep& out,
    const std::function<uint64_t(uint64_t)>& read_marker) {
    auto query = [&](RequestId request) {
        state.reconcile.push_back({request});
        out.actions.push_back({request_node(request),
            message(Kind::HandoffReconcile, 0, lock_id, request)});
    };
    for (const auto& entry : state.prepared) {
        if (excluded(entry.request)) {
            ++recovery_stats_.failed_prepared;
            continue;
        }
        const auto marker = read_marker(entry.log_slot);
        if (marker && marker != entry.request)
            throw std::runtime_error("release-marker request mismatch: fail closed");
        if (marker == entry.request) {
            auto ack = message(Kind::ReleaseAck, 0, lock_id, entry.request);
            ack.position = entry.position;
            out.actions.push_back({request_node(entry.request), ack});
        } else {
            query(entry.request);
        }
    }
    // Preserve logical rank: prepared entries first, then waiting entries.
    // New arrivals after this snapshot remain in waiting behind this prefix.
    for (const auto& m : state.waiting) {
        if (!excluded(m.request)) query(m.request);
    }
    state.waiting.clear();
    state.repair = Repair::Reconciling;
}

void HandoffState::reconciled(const Message& reply) {
    if (!authority_us_ || message_kind(reply) != Kind::HandoffReconciled ||
        reply.lock_id >= locks_.size() || !reply.request ||
        request_node(reply.request) != reply.sender ||
        (reply.other && (request_node(reply.other) != reply.sender ||
                         request_sequence(reply.other) <= request_sequence(reply.request))))
        throw std::runtime_error("invalid handoff reconciliation reply");
    if (excluded(reply.request)) return;
    auto& state = locks_[reply.lock_id];
    if (state.repair != Repair::Reconciling) return; // obsolete reply
    auto entry = std::find_if(state.reconcile.begin(), state.reconcile.end(),
                             [&](const Reconcile& e) { return e.old == reply.request; });
    if (entry == state.reconcile.end()) return; // obsolete reply
    if (entry->answered && entry->fresh != reply.other)
        throw std::runtime_error("conflicting handoff reconciliation reply");
    entry->fresh = reply.other;
    entry->answered = true;
}

void HandoffState::finish_repair(size_t lock_id, State& state, HandoffStep& out) {
    // Restore the retained FIFO prefix without changing new-arrival order.
    for (auto i = state.reconcile.rbegin(); i != state.reconcile.rend(); ++i) {
        if (excluded(i->old)) continue;
        if (i->fresh) {
            state.waiting.push_front(message(Kind::Acquire, request_node(i->fresh), lock_id, i->fresh));
            ++recovery_stats_.retried;
        } else {
            ++recovery_stats_.executed;
        }
    }
    state.reconcile.clear();
    state.prepared.clear();
    remove_active(state);
    state.repair = Repair::Normal;
    ++recovery_stats_.repaired;
    admit_waiting(state, out);
}

HandoffStep HandoffState::recover(
    LeaseClock::time_point now, const std::function<uint64_t(uint64_t)>& read_marker) {
    HandoffStep out;
    size_t i = 0;
    while (i < recovering_locks_.size()) {
        const auto lock_id = recovering_locks_[i];
        auto& state = locks_[lock_id];
        if (state.repair == Repair::Draining && !state.uncommitted) {
            bool safe = true;
            for (const auto& entry : state.prepared) {
                const auto marker = read_marker(entry.log_slot);
                if (marker && marker != entry.request)
                    throw std::runtime_error("release-marker request mismatch: fail closed");
                if (!marker && now < entry.expires) safe = false;
            }
            if (safe) {
                state.safe_at = now;
                recovery_stats_.max_safe_wait_ns = std::max(recovery_stats_.max_safe_wait_ns,
                    static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                        now - state.repair_started).count()));
                query_survivors(lock_id, state, out, read_marker);
            }
        }
        if (state.repair == Repair::Reconciling &&
            std::all_of(state.reconcile.begin(), state.reconcile.end(),
                        [](const Reconcile& e) { return e.answered; })) {
            recovery_stats_.max_repair_ns = std::max(recovery_stats_.max_repair_ns,
                static_cast<uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(
                    now - state.safe_at).count()));
            finish_repair(lock_id, state, out);
            recovering_locks_[i] = recovering_locks_.back();
            recovering_locks_.pop_back();
        } else {
            ++i;
        }
    }
    return out;
}
} // namespace lock
