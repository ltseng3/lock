#pragma once
#include "lock/locks/mu_state.h"
#include "lock/locks/lease_monitor.h"
#include "lock/locks/replicated_log.h"
#include <array>
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
struct HandoffRecoveryStats {
    uint64_t started = 0, repaired = 0, failed_waiting = 0;
    uint64_t failed_prepared = 0, retried = 0, executed = 0;
    uint64_t expired_heads = 0, expired_notices = 0;
    uint64_t max_safe_wait_ns = 0, max_repair_ns = 0;
};
class HandoffState {
public:
    // Nonzero authority_us enables recovery; supplied by conservative lease
    // arithmetic. The state machine never reads a clock itself.
    HandoffState(size_t locks, size_t prepared_window, bool gc = false,
                 uint64_t authority_us = 0);
    HandoffStep admit(const Message& acquire);
    std::vector<Action> committed(const Committed& item, LeaseClock::time_point now = {});
    HandoffStep retire(const std::function<uint64_t(uint64_t)>& read_marker);
    // Check just the committed holder(s) of this lock, particularly before
    // assigning a predecessor to a newly committed request on the same lock.
    HandoffStep retire_lock(LockId lock_id,
                            const std::function<uint64_t(uint64_t)>& read_marker);
    // Observation only: never changes ownership.
    void monitor_heads(LeaseClock::time_point now, uint64_t timeout_us, LeaseScanStats& stats);
    void exclude_client(NodeId peer, LeaseClock::time_point now);
    void expired(const Message& notice, LeaseClock::time_point now);
    void expire_heads(LeaseClock::time_point now);
    void reconciled(const Message& reply);
    HandoffStep recover(LeaseClock::time_point now,
                        const std::function<uint64_t(uint64_t)>& read_marker);
    bool recovering() const { return !recovering_locks_.empty(); }
    bool idle() const; // shutdown only
    const HandoffRecoveryStats& recovery_stats() const { return recovery_stats_; }
private:
    std::vector<ProgressWatch> lease_heads_;
    struct Entry {
        RequestId request;
        uint64_t position, log_slot;
        LeaseClock::time_point expires{};
    };
    enum class Repair { Normal, Draining, Reconciling };
    struct Reconcile {
        RequestId old, fresh = 0;
        bool answered = false;
    };
    struct State {
        uint64_t next_position = 0;
        size_t uncommitted = 0;
        std::deque<Entry> prepared;
        std::deque<Message> waiting;
        // O(1) removal from the list of locks that have a committed holder.
        size_t active_index = static_cast<size_t>(-1);
        Repair repair = Repair::Normal;
        LeaseClock::time_point repair_started{}, safe_at{};
        std::vector<Reconcile> reconcile;
    };
    size_t window_;
    bool gc_;
    uint64_t authority_us_;
    std::vector<State> locks_;
    std::array<bool, 256> excluded_{};
    std::vector<size_t> recovering_locks_;
    HandoffRecoveryStats recovery_stats_;
    bool excluded(RequestId request) const;
    void begin_repair(size_t lock_id, LeaseClock::time_point now);
    void remove_active(State& state);
    void query_survivors(size_t lock_id, State& state, HandoffStep& out,
                         const std::function<uint64_t(uint64_t)>& read_marker);
    void finish_repair(size_t lock_id, State& state, HandoffStep& out);
    // Only locks with committed, not-yet-retired entries need marker polling.
    // The replication thread exclusively owns this list and every State.
    std::vector<size_t> active_locks_;
    void admit_waiting(State& state, HandoffStep& out);
    void retire_one(size_t lock_id,
                    const std::function<uint64_t(uint64_t)>& read_marker,
                    HandoffStep& out);
};
} // namespace lock
