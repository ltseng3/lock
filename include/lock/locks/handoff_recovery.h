#pragma once
#include "lock/locks/handoff_state.h"
#include <array>
#include <cstdint>
#include <vector>

namespace lock {
// These are states at a quiesced snapshot, not at failure detection.
// Empty critical sections cannot remain in execution across an event-loop
// boundary. Finished covers a completed CS with a deferred marker.
enum class HandoffReportPhase : uint32_t { Pending = 1, Ordered = 2, Finished = 3 };
struct HandoffClientSnapshot {
    NodeId client = 0;
    uint64_t issued_through = 0;
    std::vector<Message> requests; // RecoveryHandoffRequest
};
struct HandoffPromotion {
    HandoffState state;
    std::vector<Message> pending;
    std::vector<Action> relationships;
};

// Standby-only index of immutable acquisition history. Per-lock ordinals
// validate that unfinished acquisitions form a suffix, without scanning
// historical entries at recovery. Slots are zero based; zero slot_plus_one
// denotes absence. No release inference occurs during background replay.
class HandoffRecoveryIndex {
public:
    HandoffRecoveryIndex(size_t locks, size_t max_slots);
    void apply(const Message& acquire);
    uint64_t applied() const { return applied_; }

    // Deliberately requires every configured client. A missing snapshot is
    // not evidence of completion. Future exclusion requires fencing/leases.
    HandoffPromotion reconcile(const std::vector<NodeId>& clients,
                               const std::vector<HandoffClientSnapshot>& snapshots,
                               size_t prepared_window) const;
private:
    struct Entry {
        uint64_t slot_plus_one = 0;
        uint32_t lock = 0;
        uint32_t ordinal = 0;
    };
    const Entry* find(RequestId id) const;
    std::array<std::vector<Entry>, 256> index_;
    std::array<uint64_t, 256> highest_{};
    std::vector<uint32_t> counts_;
    size_t max_slots_;
    uint64_t applied_ = 0;
};
} // namespace lock
