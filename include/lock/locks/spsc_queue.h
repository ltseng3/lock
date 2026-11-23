#pragma once
#include <atomic>
#include <cstddef>
#include <type_traits>
#include <vector>

namespace lock {
// Single producer / single consumer. Each side exclusively writes its own
// cursor. Release/acquire ordering publishes complete elements before reading.
// The caller must reject overflow or apply explicit backpressure; no drops.
template <typename T, size_t Capacity = 8192>
class SpscQueue {
    static_assert(Capacity > 1);
    static_assert(std::is_trivially_copyable_v<T>);
public:
    SpscQueue() : data_(Capacity) {}
    SpscQueue(const SpscQueue&) = delete;
    SpscQueue& operator=(const SpscQueue&) = delete;

    bool try_push(const T& value) {
        const size_t end = tail_.load(std::memory_order_relaxed);
        if (end - head_.load(std::memory_order_acquire) >= Capacity) return false;
        data_[end % Capacity] = value;
        tail_.store(end + 1, std::memory_order_release);
        return true;
    }
    bool try_pop(T& value) {
        const size_t begin = head_.load(std::memory_order_relaxed);
        if (begin == tail_.load(std::memory_order_acquire)) return false;
        value = data_[begin % Capacity];
        head_.store(begin + 1, std::memory_order_release);
        return true;
    }
    size_t size() const {
        const size_t end = tail_.load(std::memory_order_acquire);
        const size_t begin = head_.load(std::memory_order_acquire);
        return end - begin;
    }
private:
    alignas(64) std::atomic<size_t> head_{0};
    alignas(64) std::atomic<size_t> tail_{0};
    std::vector<T> data_;
};
} // namespace lock
