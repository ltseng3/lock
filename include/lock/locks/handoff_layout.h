#pragma once
#include "lock/rdma/transport.h"
#include <cstddef>
#include <cstdint>
#include <stdexcept>

namespace lock {
// Fixed server-memory layout for handoff. Header slots occupy the first half
// of the log area; matching retirement markers occupy the adjacent half.
// The last 16 MiB is reserved for Transport's local scratch allocations.
//
//   kLogOffset + slot * 8             : replicated 8-byte header
//   kLogOffset + capacity * 8 + slot*8: leader-side 8-byte release marker
//
// Both addresses are 8-byte aligned. Header slots are append-only; marker
// slots begin zeroed and are written exactly once by the holder.
constexpr size_t kHandoffScratchReserve = 16ULL * 1024 * 1024;
inline size_t handoff_log_capacity(size_t server_memory_bytes) {
    if (server_memory_bytes <= rdma::kLogOffset + kHandoffScratchReserve + 16)
        throw std::invalid_argument("handoff server memory is too small");
    return (server_memory_bytes - kHandoffScratchReserve - rdma::kLogOffset) /
           (2 * sizeof(uint64_t));
}
inline size_t handoff_header_offset(size_t server_memory_bytes, uint64_t slot) {
    if (slot >= handoff_log_capacity(server_memory_bytes))
        throw std::out_of_range("handoff header slot out of range");
    return rdma::kLogOffset + static_cast<size_t>(slot) * sizeof(uint64_t);
}
inline size_t handoff_marker_offset(size_t server_memory_bytes, uint64_t slot) {
    const size_t capacity = handoff_log_capacity(server_memory_bytes);
    if (slot >= capacity)
        throw std::out_of_range("handoff marker slot out of range");
    return rdma::kLogOffset + (capacity + static_cast<size_t>(slot)) *
           sizeof(uint64_t);
}
} // namespace lock
