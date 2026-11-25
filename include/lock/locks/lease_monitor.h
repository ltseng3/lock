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

// Bound a client-local activation window plus holding lease in leader-local
// microseconds. Both clocks must advance at rates in [1-rho, 1+rho], where
// rho is max_clock_error_ppm / 1e6. Submission (or recovery-ready) must
// causally precede the leader's timestamp. Round upward; network delay does
// not enter the bound. This helper does not establish the clock assumption.
inline uint64_t conservative_authority_us(uint64_t activation_us, uint64_t hold_us,
                                          uint64_t max_clock_error_ppm) {
    constexpr uint64_t scale = 1'000'000;
    constexpr uint64_t max_duration_us = 60'000'000;
    if (!activation_us || !hold_us || activation_us > max_duration_us ||
        hold_us > max_duration_us || max_clock_error_ppm >= scale)
        throw std::invalid_argument("invalid authority bound");
    // Inputs are bounded above; the multiplication fits in uint64_t.
    const uint64_t denominator = scale - max_clock_error_ppm;
    const uint64_t numerator = (activation_us + hold_us) * (scale + max_clock_error_ppm);
    return numerator / denominator + (numerator % denominator != 0);
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
    bool sample(ProgressWatch& watch, uint64_t token, LeaseClock::time_point now,
                uint64_t timeout_us) {
        ++checks;
        changes += watch.observe(token, now);
        max_age_ns = std::max(max_age_ns, static_cast<uint64_t>(
            std::chrono::duration_cast<std::chrono::nanoseconds>(now - watch.since).count()));
        const bool suspected = watch.suspect(now, timeout_us);
        suspicions += suspected;
        return suspected;
    }
};
// Disjoint from flags, application data (1000 words), and log.
constexpr size_t kHeartbeatBase = rdma::kHeartbeatBase;
constexpr size_t kHeartbeatStride = rdma::kHeartbeatStride;
inline size_t heartbeat_offset(NodeId client) {
    if (!client || client >= rdma::kHeartbeatSlots)
        throw std::invalid_argument("invalid heartbeat client");
    return kHeartbeatBase + client * kHeartbeatStride;
}
struct HandoffLeaseConfig {
    uint64_t duration_us = 0, scan_us = 0, heartbeat_us = 0;
    bool client_recovery = false;
    uint64_t activation_us = 0, clock_error_ppm = 1'000'000;
    uint64_t authority_us() const {
        return client_recovery
            ? conservative_authority_us(activation_us, duration_us, clock_error_ppm) : 0;
    }
    void validate() const {
        if (duration_us > 60'000'000 || scan_us > 60'000'000 ||
            heartbeat_us > 60'000'000 || (scan_us && !duration_us))
            throw std::invalid_argument("invalid Handoff lease configuration");
        if (client_recovery) {
            if (!scan_us && !heartbeat_us)
                throw std::invalid_argument("client recovery requires monitoring");
            (void)authority_us();
        } else if (activation_us || clock_error_ppm != 1'000'000) {
            throw std::invalid_argument("activation and clock bound require client recovery");
        }
    }
};
} // namespace lock
