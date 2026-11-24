#pragma once
#include "lock/locks/compact_recovery.h"
#include "lock/locks/mu_state.h"
#include <array>
#include <stdexcept>
#include <vector>

namespace lock {
enum class MuRequestStatus : uint8_t { Absent, Waiting, Granted, Released };
struct MuRequestInfo {
    MuRequestStatus status = MuRequestStatus::Absent;
    LockId lock = 0;
};
// Dense per-client sequence index. Only standbys allocate it. Each status
// uses 4 bytes, with a bound derived from the finite append-only log.
class MuRecoveryState {
public:
    MuRecoveryState(size_t locks, size_t limit) : state_(locks), limit_(limit) {}
    void apply(const Message& m) {
        auto old = lookup(m.request);
        const auto kind = message_kind(m);
        if (kind == Kind::Acquire) {
            if (old.status != MuRequestStatus::Absent)
                throw std::runtime_error("duplicate recovered acquire");
            set(m.request, m.lock_id, MuRequestStatus::Waiting);
            ++active_;
        } else if (kind == Kind::Release) {
            if (old.status != MuRequestStatus::Granted || old.lock != m.lock_id)
                throw std::runtime_error("recovered release from nonholder");
            set(m.request, m.lock_id, MuRequestStatus::Released);
            --active_;
        } else throw std::runtime_error("invalid recovery mutation");
        for (const auto& a : state_.apply(m)) {
            if (message_kind(a.message) == Kind::Grant)
                set(a.message.request, a.message.lock_id, MuRequestStatus::Granted);
        }
        ++applied_;
    }
    MuRequestInfo lookup(RequestId id) const {
        const auto n = request_node(id);
        const auto seq = request_sequence(id);
        if (!n || n >= index_.size() || !seq)
            throw std::runtime_error("invalid recovery request ID");
        if (seq >= index_[n].size()) return {};
        uint32_t encoded = index_[n][seq];
        return {static_cast<MuRequestStatus>(encoded >> 16), encoded & 0xffff};
    }
    uint64_t applied() const { return applied_; }
    size_t active() const { return active_; }
    MuState take_state() { return std::move(state_); }
private:
    void set(RequestId id, LockId lock, MuRequestStatus status) {
        auto node = request_node(id);
        auto seq = request_sequence(id);
        if (!node || node >= index_.size() || !seq || seq > limit_ ||
            lock > kMuMaxLockId)
            throw std::runtime_error("recovery index bound exceeded");
        auto& records = index_[node];
        if (records.size() <= seq) records.resize(seq + 1);
        records[seq] = (static_cast<uint32_t>(status) << 16) | lock;
    }
    MuState state_;
    size_t limit_, active_ = 0;
    uint64_t applied_ = 0;
    std::array<std::vector<uint32_t>, 256> index_;
};
enum class MuRecoveryAction { RetryAcquire, Wait, Grant, RetryRelease, Ack };
inline MuRecoveryAction reconcile_mu(MuRequestInfo info, LockId lock, bool releasing) {
    if (info.status != MuRequestStatus::Absent && info.lock != lock)
        throw std::runtime_error("recovery request lock mismatch");
    if (releasing) {
        if (info.status == MuRequestStatus::Granted) return MuRecoveryAction::RetryRelease;
        if (info.status == MuRequestStatus::Released) return MuRecoveryAction::Ack;
    } else {
        if (info.status == MuRequestStatus::Absent) return MuRecoveryAction::RetryAcquire;
        if (info.status == MuRequestStatus::Waiting) return MuRecoveryAction::Wait;
        if (info.status == MuRequestStatus::Granted) return MuRecoveryAction::Grant;
    }
    throw std::runtime_error("client phase inconsistent with recovered log");
}
} // namespace lock
