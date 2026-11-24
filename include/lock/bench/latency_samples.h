#pragma once
#include <algorithm>
#include <cmath>
#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <vector>

namespace lock {
struct LatencyPercentiles {
    uint64_t p50 = 0, p99 = 0, p999 = 0;
};
struct LatencySummary {
    LatencyPercentiles acquire, total;
};

// Exact per-completion samples, with a fixed, initialized storage budget.
// Allocation and first-touch happen before the client start barrier.
class LatencySamples {
public:
    static constexpr size_t max_capacity = 64000000;
    static constexpr size_t default_samples_per_second = 1500000;

    static size_t capacity_for(uint64_t seconds, uint64_t requested = 0) {
        if (requested) {
            if (requested > max_capacity)
                throw std::invalid_argument("latency-sample-capacity exceeds 64000000");
            return static_cast<size_t>(requested);
        }
        if (!seconds || seconds > max_capacity / default_samples_per_second)
            throw std::invalid_argument("set --latency-sample-capacity explicitly for this duration");
        return static_cast<size_t>(seconds) * default_samples_per_second;
    }

    void prepare(size_t capacity) {
        if (!capacity || capacity > max_capacity || !acquire_.empty())
            throw std::invalid_argument("invalid latency buffer initialization");
        // Unlike reserve(), resize() initializes all elements, writing the
        // pages before measurement. No growth or copying occurs on record().
        acquire_.resize(capacity, 0);
        total_.resize(capacity, 0);
    }

    void record(uint64_t acquire, uint64_t total) {
        if (size_ == capacity())
            throw std::runtime_error(
                "latency sample capacity exhausted: increase --latency-sample-capacity");
        acquire_[size_] = acquire;
        total_[size_] = total;
        ++size_;
    }

    size_t size() const { return size_; }
    size_t capacity() const { return acquire_.size(); }

    // Called only after all clients finish. Sort the populated ranges once,
    // not six full copies of the measurement vectors.
    LatencySummary summarize() {
        return {percentiles(acquire_), percentiles(total_)};
    }

private:
    LatencyPercentiles percentiles(std::vector<uint64_t>& samples) const {
        if (!size_) return {};
        std::sort(samples.begin(), samples.begin() + size_);
        const auto at = [&](double p) {
            const size_t index = std::min(size_ - 1,
                static_cast<size_t>(std::ceil(p * size_)) - 1);
            return samples[index];
        };
        return {at(0.50), at(0.99), at(0.999)};
    }

    std::vector<uint64_t> acquire_, total_;
    size_t size_ = 0;
};
} // namespace lock
