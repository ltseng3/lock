#include "lock/locks/replicated_log.h"
#include <algorithm>
#include <stdexcept>
namespace lock {
ReplicatedLog::ReplicatedLog(rdma::Transport& net, std::vector<NodeId> replicas)
    : net_(net), replicas_(std::move(replicas)) {
    if (replicas_.size() != 3 && replicas_.size() != 5)
        throw std::invalid_argument("only three or five replicas are supported");
    if (std::find(replicas_.begin(), replicas_.end(), net.self()) == replicas_.end())
        throw std::invalid_argument("leader is not a replica");
    const size_t scratch_start = net.memory_bytes() - 16ULL * 1024 * 1024;
    capacity_ = (scratch_start - rdma::kLogOffset) / sizeof(LogRecord);
}
uint64_t ReplicatedLog::append(const Message& m) {
    if (pending_.size() >= capacity_)
        throw std::runtime_error("replicated log exhausted: fail closed");
    uint64_t slot = pending_.size();
    size_t offset = rdma::kLogOffset + slot * sizeof(LogRecord);
    auto* record = static_cast<LogRecord*>(net_.at(offset, sizeof(LogRecord)));
    *record = {};
    record->command = m;
    pending_.push_back({1, m});
    for (NodeId peer : replicas_) {
        if (peer == net_.self()) continue;
        WorkId id = next_work_++;
        writes_.emplace(id, slot);
        net_.write(peer, record, sizeof(LogRecord), offset, id);
    }
    return slot;
}
void ReplicatedLog::on_write_complete(WorkId id) {
    auto it = writes_.find(id);
    if (it == writes_.end()) throw std::runtime_error("unexpected replication work ID");
    auto slot = it->second;
    writes_.erase(it);
    if (slot >= pending_.size()) throw std::logic_error("replication slot disappeared");
    ++pending_[slot].acknowledgments;
}
std::optional<Committed> ReplicatedLog::next_committed() {
    if (applied_ == pending_.size()) return std::nullopt;
    if (pending_[applied_].acknowledgments < replicas_.size() / 2 + 1)
        return std::nullopt;
    uint64_t slot = applied_++;
    return Committed{slot, pending_[slot].command};
}
} // namespace lock
