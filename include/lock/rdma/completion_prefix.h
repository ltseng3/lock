#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <stdexcept>

namespace lock::rdma {
// Single-QP FIFO of signaled batch-end tokens. Tokens are exclusive,
// monotonically increasing counters in a caller-defined unit (log entries
// or SEND WRs). All signaled CQEs must be observed in QP order. Fixed storage
// avoids allocation on the post/completion path. Not recovery metadata.
template <size_t Capacity>
class CompletionPrefix {
    static_assert(Capacity > 0);
public:
    bool full() const { return pending_ == Capacity; }
    uint64_t posted() const { return posted_; }
    uint64_t completed() const { return completed_; }
    void post(uint64_t end) {
        if (full() || end <= posted_)
            throw std::runtime_error("invalid QP post prefix");
        ends_[(head_ + pending_) % Capacity] = end;
        ++pending_;
        posted_ = end;
    }
    void complete(uint64_t end) {
        if (!pending_ || ends_[head_] != end || end <= completed_)
            throw std::runtime_error("invalid QP completion prefix");
        head_ = (head_ + 1) % Capacity;
        --pending_;
        completed_ = end;
    }
private:
    std::array<uint64_t, Capacity> ends_{};
    size_t head_ = 0, pending_ = 0;
    uint64_t posted_ = 0, completed_ = 0;
};
} // namespace lock::rdma
