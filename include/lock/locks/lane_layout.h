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
// Partition physical log slots into disjoint contiguous lane ranges.
// Round down to a cache line (8 compact words), so neighboring lanes
// never write the same header cache line. The small remainder is unused.
class CompactLaneLayout {
public:
    CompactLaneLayout(size_t capacity, size_t lane, size_t lane_count)
        : lane_(lane) {
        if (!lane_count || lane_count > 8 || lane >= lane_count)
            throw std::invalid_argument("invalid compact log lane");
        slots_ = (capacity / lane_count / 8) * 8;
        if (!slots_) throw std::invalid_argument("empty compact lane range");
    }
    size_t capacity() const { return slots_; }
    uint64_t global_slot(uint64_t local) const {
        if (local >= slots_)
            throw std::out_of_range("compact lane log exhausted");
        return lane_ * slots_ + local;
    }
private:
    size_t lane_, slots_;
};
} // namespace lock
