#include "lock/locks/lane_layout.h"
#include "lock/locks/handoff_layout.h"
#include "lock/types.h"
#include <array>
#include <cassert>
#include <cstdint>
#include <set>
#include <stdexcept>
int main() {
    using namespace lock;
    constexpr size_t bytes = 512ULL * 1024 * 1024;
    for (size_t lane_count : {1u, 2u, 4u, 8u}) {
        std::set<uint64_t> assigned_slots;
        for (uint32_t lock_id = 0; lock_id < 1024; ++lock_id)
            assert(lock_lane(lock_id, lane_count) == lock_id % lane_count);
        for (size_t lane = 0; lane < lane_count; ++lane) {
            uint64_t previous = 0;
            for (uint64_t local = 0; local < 100; ++local) {
                const auto slot = compact_global_slot(local, lane, lane_count);
                assert(slot % lane_count == lane);
                assert(assigned_slots.insert(slot).second);
                if (local) assert(slot == previous + lane_count);
                previous = slot;
                const auto header = handoff_header_offset(bytes, slot);
                const auto marker = handoff_marker_offset(bytes, slot);
                assert(header % 8 == 0 && marker % 8 == 0);
                assert(header < marker);
            }
        }
        assert(assigned_slots.size() == 100 * lane_count);
    }
    bool invalid = false;
    try { (void)lock_lane(3, 0); }
    catch (const std::invalid_argument&) { invalid = true; }
    assert(invalid);
    invalid = false;
    try { (void)compact_global_slot(UINT64_MAX, 3, 4); }
    catch (const std::out_of_range&) { invalid = true; }
    assert(invalid);
}
