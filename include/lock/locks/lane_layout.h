#pragma once
#include <cstddef>
#include <cstdint>
#include <stdexcept>

namespace lock {
// Each lock belongs to precisely one independent log lane. A lane has its
// own quorum QPs, CQ, commit prefix and lock-state owner.
inline size_t lock_lane(uint32_t lock_id, size_t lane_count) {
    if (lane_count == 0 || lane_count > 8)
        throw std::invalid_argument("lane count must be 1..8");
    return lock_id % lane_count;
}
inline uint64_t compact_global_slot(uint64_t local_slot, size_t lane,
                                    size_t lane_count) {
    if (lane_count == 0 || lane_count > 8 || lane >= lane_count ||
        local_slot > (UINT64_MAX - lane) / lane_count)
        throw std::out_of_range("invalid compact log lane slot");
    return local_slot * lane_count + lane;
}
} // namespace lock
