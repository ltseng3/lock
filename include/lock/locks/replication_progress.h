#pragma once
#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include "lock/rdma/completion_prefix.h"
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
    // In a recovered three-replica configuration exactly one surviving
    // follower remains. The original majority still requires one remote copy.
    explicit ReplicationProgress(size_t followers, uint64_t recovered = 0,
                                 size_t required = 0)
        : followers_(followers, rdma::CompletionPrefix<max_outstanding_batches>(recovered)),
          required_(required ? required : followers / 2),
          posted_(recovered), committed_(recovered) {
        if ((!required && followers != 2 && followers != 4) ||
            (required && (followers != 1 || required != 1)))
            throw std::invalid_argument("invalid fixed-membership replication quorum");
    }
    bool can_post() const {
        for (const auto& f : followers_)
            if (f.full()) return false;
        return true;
    }
    // Call after a batch is successfully posted to every follower.
    // Partial posting is fatal; callers must not retry or publish a commit.
    void posted(uint64_t end) {
        if (end <= posted_ || !can_post())
            throw std::runtime_error("invalid replication batch end or credit");
        for (auto& f : followers_) f.post(end);
        posted_ = end;
    }
    void completed(size_t follower, uint64_t end) {
        followers_.at(follower).complete(end);
        std::array<uint64_t, 4> prefixes{};
        for (size_t i = 0; i < followers_.size(); ++i)
            prefixes[i] = followers_[i].completed();
        // Leader contributes one copy. The quorum-th highest follower
        // prefix is the largest contiguous prefix present on a majority.
        std::sort(prefixes.begin(), prefixes.end(),
                  std::greater<uint64_t>{});
        committed_ = prefixes[required_ - 1];
    }
    uint64_t committed() const { return committed_; }
    uint64_t posted() const { return posted_; }
private:
    std::vector<rdma::CompletionPrefix<max_outstanding_batches>> followers_;
    size_t required_;
    uint64_t posted_ = 0, committed_ = 0;
};
} // namespace lock
