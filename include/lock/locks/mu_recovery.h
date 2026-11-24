#pragma once
#include "lock/locks/lane_layout.h"
#include "lock/locks/epoch_boundary.h"
#include "lock/locks/mu_encoding.h"
#include "lock/locks/mu_state.h"
#include "lock/rdma/transport.h"
#include <algorithm>
#include <array>
#include <atomic>
#include <stdexcept>
#include <vector>

namespace lock {
// Recovery experiments reserve a byte of publication metadata per word.
// Raw mode uses the identical layout but assumes atomic aligned word DMA.
// The ordinary no-recovery layout is unchanged.
inline size_t mu_recovery_capacity(size_t memory) {
    const size_t end = epoch_boundary_base(memory);
    if (end <= rdma::kLogOffset)
        throw std::invalid_argument("recovery log memory too small");
    return (end - rdma::kLogOffset) / 9;
}
inline size_t mu_recovery_marker_offset(size_t memory, uint64_t slot) {
    const size_t capacity = mu_recovery_capacity(memory);
    if (slot >= capacity) throw std::out_of_range("recovery marker slot");
    return rdma::kLogOffset + capacity * sizeof(uint64_t) + slot;
}
inline uint64_t safe_replay_prefix(uint64_t observed, size_t window) {
    if (!window) throw std::invalid_argument("zero replay window");
    return observed > window ? observed - window : 0;
}
struct MuRecoveryConfig {
    bool enabled = false;
    bool publication = true; // ordered sidecar WRITE, not a second round
    bool replay = true;
    uint64_t recovered = 0;
    bool promoted = false;
};
// Single writer, append-only, one crash-stop. Never scan past a hole.
// Publication mode: each byte is written on the same QP after the payload.
// Raw mode requires atomic aligned 8-byte DMA and ordered CPU visibility;
// it is an experimental hardware assumption, not a portable guarantee.
class MuLogView {
public:
    MuLogView(rdma::Transport& net, bool publication, size_t lane = 0,
              size_t lanes = 1)
        : layout_(mu_recovery_capacity(net.memory_bytes()), lane, lanes),
          words_(static_cast<uint64_t*>(net.at(
              rdma::kLogOffset + layout_.global_slot(0) * 8, layout_.capacity() * 8))),
          marks_(static_cast<uint8_t*>(net.at(mu_recovery_marker_offset(
              net.memory_bytes(), layout_.global_slot(0)), layout_.capacity()))),
          publication_(publication) {}
    size_t capacity() const { return layout_.capacity(); }
    uint64_t global_slot(uint64_t i) const { return layout_.global_slot(i); }
    uint64_t word(uint64_t i) const {
        if (i >= capacity()) throw std::out_of_range("recovery word");
        return std::atomic_ref<uint64_t>(words_[i]).load(std::memory_order_acquire);
    }
    bool present(uint64_t i) const {
        if (i >= capacity()) throw std::out_of_range("recovery presence");
        if (!publication_) return word(i) != 0;
        auto m = std::atomic_ref<uint8_t>(marks_[i]).load(std::memory_order_acquire);
        if (m > 1) throw std::runtime_error("invalid publication byte");
        return m == 1;
    }
    uint64_t scan(uint64_t prefix, size_t budget = static_cast<size_t>(-1)) const {
        if (prefix > capacity()) throw std::out_of_range("recovery frontier");
        while (prefix < capacity() && budget && present(prefix)) {
            ++prefix;
            --budget;
        }
        return prefix;
    }
    Message command(uint64_t i) const { return unpack_mu_command(word(i)); }
    uint64_t* words(uint64_t begin, size_t count) const {
        if (begin > capacity() || count > capacity() - begin)
            throw std::out_of_range("recovery words range");
        return words_ + begin;
    }
    uint8_t* marks(uint64_t begin, size_t count) const {
        if (begin > capacity() || count > capacity() - begin)
            throw std::out_of_range("recovery marker range");
        return marks_ + begin;
    }
private:
    CompactLaneLayout layout_;
    uint64_t* words_;
    uint8_t* marks_;
    bool publication_;
};
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
