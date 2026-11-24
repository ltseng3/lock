#pragma once
#include "lock/types.h"
#include <cstddef>
#include <cstdint>
#include <span>
#include <stdexcept>

namespace lock {
// Recovery-only registered-memory layout: the boundary table is immediately
// before the transport's last-16-MiB scratch area, outside all log arrays.
// Epoch zero starts at slot zero implicitly. Each following epoch occupies
// one aligned eight-byte record (epoch, exclusive previous-prefix length).
// This table is metadata, not a consensus/election implementation.
constexpr size_t kRecoveryScratchBytes = 16ULL * 1024 * 1024;
constexpr size_t kEpochBoundaryBytes = 4096;
constexpr size_t kEpochBoundaryRecords = kEpochBoundaryBytes / sizeof(uint64_t);
constexpr uint64_t kEpochSlotMask = (1ULL << 48) - 1;

inline size_t epoch_boundary_base(size_t memory) {
    if (memory <= kRecoveryScratchBytes + kEpochBoundaryBytes)
        throw std::invalid_argument("memory too small for epoch boundary table");
    return memory - kRecoveryScratchBytes - kEpochBoundaryBytes;
}
inline size_t epoch_boundary_offset(size_t memory, Epoch epoch) {
    if (epoch == kInitialEpoch || epoch > kEpochBoundaryRecords)
        throw std::invalid_argument("epoch boundary table exhausted");
    const auto base = epoch_boundary_base(memory);
    if (base % alignof(uint64_t))
        throw std::invalid_argument("unaligned epoch boundary table");
    return base + (static_cast<size_t>(epoch) - 1) * sizeof(uint64_t);
}
struct EpochBoundary {
    Epoch epoch;
    uint64_t first_slot;
};
inline uint64_t encode_epoch_boundary(EpochBoundary b) {
    if (!b.epoch || b.epoch > kEpochBoundaryRecords || b.first_slot > kEpochSlotMask)
        throw std::invalid_argument("invalid epoch boundary");
    return (static_cast<uint64_t>(b.epoch) << 48) | b.first_slot;
}
inline EpochBoundary decode_epoch_boundary(uint64_t word) {
    EpochBoundary b{static_cast<Epoch>(word >> 48), word & kEpochSlotMask};
    (void)encode_epoch_boundary(b);
    return b;
}
// Use only a quorum-established boundary table; raw nonzero metadata does
// not prove commitment. Equal starts are legal for epochs with no commands.
inline Epoch epoch_at(std::span<const uint64_t> committed_boundaries, uint64_t slot) {
    Epoch result = kInitialEpoch;
    uint64_t previous = 0;
    Epoch expected = 1;
    for (uint64_t word : committed_boundaries) {
        auto b = decode_epoch_boundary(word);
        if (b.epoch != expected++ || b.first_slot < previous)
            throw std::runtime_error("invalid epoch history");
        previous = b.first_slot;
        if (b.first_slot <= slot) result = b.epoch;
    }
    return result;
}
} // namespace lock
