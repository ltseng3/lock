#pragma once
#include "lock/rdma/transport.h"
#include <algorithm>
#include <chrono>
#include <cstdint>
#include <stdexcept>

namespace lock {
using LeaseClock = std::chrono::steady_clock;
inline bool lease_elapsed(LeaseClock::time_point acquired,
                          LeaseClock::time_point now, uint64_t duration_us) {
    return duration_us && now - acquired >= std::chrono::microseconds(duration_us);
}

// Observation age is NOT the holder's lease age. No timeout grants authority.
struct ProgressWatch {
    uint64_t token = 0;
    LeaseClock::time_point since{};
    bool initialized = false, reported = false;
    bool observe(uint64_t next, LeaseClock::time_point now) {
        if (initialized && token == next) return false;
        initialized = true; token = next; since = now; reported = false;
        return true;
    }
    bool suspect(LeaseClock::time_point now, uint64_t timeout_us) {
        if (!initialized || reported || !lease_elapsed(since, now, timeout_us)) return false;
        reported = true;
        return true;
    }
};
struct LeaseScanStats {
    uint64_t scans = 0, checks = 0, changes = 0, suspicions = 0, max_age_ns = 0;
    void sample(ProgressWatch& watch, uint64_t token, LeaseClock::time_point now,
                uint64_t timeout_us) {
        ++checks;
        changes += watch.observe(token, now);
        max_age_ns = std::max(max_age_ns, static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(now - watch.since).count()));
        suspicions += watch.suspect(now, timeout_us);
    }
};
// Disjoint from flags, application data (1000 words), and log.
constexpr size_t kHeartbeatBase = 0x40000, kHeartbeatStride = 64;
static_assert(rdma::kDataOffset + 1000 * sizeof(uint64_t) <= kHeartbeatBase);
static_assert(kHeartbeatBase + 256 * kHeartbeatStride <= rdma::kLogOffset);
inline size_t heartbeat_offset(NodeId client) {
    if (!client || client >= 256) throw std::invalid_argument("invalid heartbeat client");
    return kHeartbeatBase + client * kHeartbeatStride;
}
struct HandoffLeaseConfig {
    uint64_t duration_us = 0, scan_us = 0, heartbeat_us = 0;
    void validate() const {
        if (duration_us > 60'000'000 || scan_us > 60'000'000 ||
            heartbeat_us > 60'000'000 || (scan_us && !duration_us))
            throw std::invalid_argument("invalid Handoff lease configuration");
    }
};
} // namespace lock
