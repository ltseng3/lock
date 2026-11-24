#pragma once
#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <stdexcept>
#include <vector>

namespace lock {
// Failure-free RC replication progress for ONE lane. A prefix is exclusive:
// prefix=N means all local slots [0,N) have completed on that follower.
// Every posted batch is signaled; a CQE identifies the end of that batch.
// No state here is durable election/recovery metadata.
class ReplicationProgress {
public:
    static constexpr size_t max_outstanding_batches = 512;
    explicit ReplicationProgress(size_t followers) : followers_(followers) {
        if (followers != 2 && followers != 4)
            throw std::invalid_argument("replication requires 2 or 4 followers");
    }
    bool can_post() const {
        for (const auto& f : followers_)
            if (f.pending.size() >= max_outstanding_batches) return false;
        return true;
    }
    // Call after a batch is successfully posted to every follower.
    // Partial posting is fatal; callers must not retry or publish a commit.
    void posted(uint64_t end) {
        if (end <= posted_ || !can_post())
            throw std::runtime_error("invalid replication batch end or credit");
        for (auto& f : followers_) f.pending.push_back(end);
        posted_ = end;
    }
    void completed(size_t follower, uint64_t end) {
        auto& f = followers_.at(follower);
        if (f.pending.empty() || f.pending.front() != end || end <= f.prefix)
            throw std::runtime_error("invalid replication batch completion");
        f.pending.pop_front();
        f.prefix = end;
        std::array<uint64_t, 4> prefixes{};
        for (size_t i = 0; i < followers_.size(); ++i)
            prefixes[i] = followers_[i].prefix;
        // Leader contributes one copy. The quorum-th highest follower
        // prefix is the largest contiguous prefix present on a majority.
        std::sort(prefixes.begin(), prefixes.begin() + followers_.size(),
                  std::greater<uint64_t>{});
        committed_ = prefixes[followers_.size() / 2 - 1];
    }
    uint64_t committed() const { return committed_; }
    uint64_t posted() const { return posted_; }
private:
    struct Follower {
        uint64_t prefix = 0;
        std::deque<uint64_t> pending;
    };
    std::vector<Follower> followers_;
    uint64_t posted_ = 0, committed_ = 0;
};
} // namespace lock
