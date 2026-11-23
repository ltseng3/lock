#include "lock/locks/mu_compact_log.h"
#include <algorithm>
#include <stdexcept>

namespace lock {
MuCompactLog::MuCompactLog(rdma::Transport& net, std::vector<NodeId> replicas,
                           size_t window, size_t capacity_limit)
    : net_(net), ring_(window) {
    if ((replicas.size() != 3 && replicas.size() != 5) ||
        std::find(replicas.begin(), replicas.end(), net.self()) == replicas.end())
        throw std::invalid_argument("Mu requires three or five replicas including leader");
    if (window == 0 || window > 4096)
        throw std::invalid_argument("invalid Mu log pipeline window");
    for (NodeId id : replicas)
        if (id != net.self()) followers_.push_back(id);
    quorum_followers_ = replicas.size() / 2;
    const size_t end = net.memory_bytes() - 16ULL * 1024 * 1024;
    capacity_ = (end - rdma::kLogOffset) / sizeof(uint64_t);
    if (capacity_limit) {
        if (capacity_limit > capacity_)
            throw std::invalid_argument("compact log capacity exceeds registered region");
        capacity_ = capacity_limit;
    }
}
uint64_t MuCompactLog::append(const Message& mutation) {
    if (inflight() >= ring_.size())
        throw std::runtime_error("Mu log admission window full");
    if (next_ >= capacity_)
        throw std::runtime_error("Mu replicated log exhausted: fail closed");
    const uint64_t word = pack_mu_command(mutation);
    const uint64_t slot = next_++;
    const size_t offset = rdma::kLogOffset + slot * sizeof(uint64_t);
    *static_cast<uint64_t*>(net_.at(offset, sizeof(uint64_t))) = word;
    auto& entry = ring_[slot % ring_.size()];
    entry = {};
    entry.slot = slot;
    entry.command = mutation;
    entry.remaining = static_cast<uint8_t>(quorum_followers_);
    for (size_t i = 0; i < followers_.size(); ++i) {
        // Rotate the majority subset to avoid always waiting on one follower.
        const size_t relative = (i + followers_.size() -
                                 slot % followers_.size()) % followers_.size();
        const bool quorum = relative < quorum_followers_;
        if (quorum) entry.quorum_mask |= static_cast<uint8_t>(1u << i);
        // Source is copied by the inline WRITE before this call returns.
        // WorkId is the log slot+1, well within the transport's 48-bit tag.
        net_.write_inline(followers_[i], word, offset, slot + 1, quorum);
    }
    return slot;
}
void MuCompactLog::on_write_complete(NodeId peer, WorkId id) {
    if (id == 0) throw std::runtime_error("invalid Mu replication work ID");
    const uint64_t slot = id - 1;
    auto found = std::find(followers_.begin(), followers_.end(), peer);
    if (found == followers_.end())
        throw std::runtime_error("Mu replication completion from non-follower");
    const size_t index = static_cast<size_t>(found - followers_.begin());
    const size_t relative = (index + followers_.size() -
                             slot % followers_.size()) % followers_.size();
    if (relative >= quorum_followers_) {
        // A periodically signaled non-quorum WRITE may complete after the
        // slot's local bookkeeping has already been reused.
        return;
    }
    if (slot < applied_ || slot >= next_)
        throw std::runtime_error("stale Mu quorum completion");
    auto& entry = ring_[slot % ring_.size()];
    const uint8_t bit = static_cast<uint8_t>(1u << index);
    if (entry.slot != slot || !(entry.quorum_mask & bit) ||
        (entry.seen_mask & bit) || entry.remaining == 0)
        throw std::runtime_error("invalid or duplicate Mu quorum completion");
    entry.seen_mask |= bit;
    --entry.remaining;
}
std::optional<Committed> MuCompactLog::next_committed() {
    if (applied_ == next_) return std::nullopt;
    auto& entry = ring_[applied_ % ring_.size()];
    if (entry.slot != applied_)
        throw std::runtime_error("Mu log slot ordering violation");
    if (entry.remaining != 0) return std::nullopt;
    return Committed{applied_++, entry.command};
}
} // namespace lock
