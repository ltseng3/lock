#pragma once
#include "lock/locks/lane_layout.h"
#include "lock/locks/epoch_boundary.h"
#include "lock/locks/mu_encoding.h"
#include "lock/rdma/transport.h"
#include <atomic>
#include <stdexcept>

namespace lock {
// Recovery experiments reserve a byte of publication metadata per word.
// Raw mode uses the identical layout but assumes atomic aligned word DMA.
// The ordinary no-recovery layout is unchanged.
// Handoff additionally has one aligned 8-byte retirement word per header.
// Publication bytes are last, separate from retirement evidence.
inline size_t compact_recovery_capacity(size_t memory, bool handoff = false) {
    const size_t end = epoch_boundary_base(memory);
    if (end <= rdma::kLogOffset)
        throw std::invalid_argument("recovery log memory too small");
    return (end - rdma::kLogOffset) / (handoff ? 17 : 9);
}
inline size_t compact_publication_offset(size_t memory, uint64_t slot, bool handoff = false) {
    const size_t capacity = compact_recovery_capacity(memory, handoff);
    if (slot >= capacity) throw std::out_of_range("recovery publication slot");
    return rdma::kLogOffset + capacity * (handoff ? 16 : 8) + slot;
}
inline uint64_t safe_replay_prefix(uint64_t observed, size_t window) {
    if (!window) throw std::invalid_argument("zero replay window");
    return observed > window ? observed - window : 0;
}
struct CompactRecoveryConfig {
    bool enabled = false;
    bool publication = true; // ordered sidecar WRITE, not a second round
    bool replay = true;
    uint64_t recovered = 0;
    bool promoted = false;
    bool handoff = false; // use the header + retirement + publication layout
};
// Single writer, append-only, one crash-stop. Never scan past a hole.
// Publication mode: each byte is written on the same QP after the payload.
// Raw mode requires atomic aligned 8-byte DMA and ordered CPU visibility;
// it is an experimental hardware assumption, not a portable guarantee.
class CompactLogView {
public:
    CompactLogView(rdma::Transport& net, bool publication, size_t lane = 0,
              size_t lanes = 1, bool handoff = false)
        : layout_(compact_recovery_capacity(net.memory_bytes(), handoff), lane, lanes),
          words_(static_cast<uint64_t*>(net.at(
              rdma::kLogOffset + layout_.global_slot(0) * 8, layout_.capacity() * 8))),
          marks_(static_cast<uint8_t*>(net.at(compact_publication_offset(
              net.memory_bytes(), layout_.global_slot(0), handoff), layout_.capacity()))),
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
} // namespace lock
