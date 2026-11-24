#include "lock/locks/mu_compact_log.h"
#include <algorithm>
#include <array>
#include <stdexcept>

namespace lock {
namespace {
size_t log_capacity(size_t memory, size_t limit) {
    constexpr size_t reserve = 16ULL * 1024 * 1024;
    if (memory <= rdma::kLogOffset + reserve)
        throw std::invalid_argument("compact log memory too small");
    const size_t capacity = (memory - reserve - rdma::kLogOffset) / sizeof(uint64_t);
    if (limit > capacity)
        throw std::invalid_argument("compact log capacity exceeds registered region");
    return limit ? limit : capacity;
}
std::vector<NodeId> follower_ids(NodeId self, std::vector<NodeId> replicas, bool promoted) {
    if ((promoted ? replicas.size() != 2 : (replicas.size() != 3 && replicas.size() != 5)) ||
        std::find(replicas.begin(), replicas.end(), self) == replicas.end())
        throw std::invalid_argument("Mu requires three or five replicas including leader");
    std::sort(replicas.begin(), replicas.end());
    if (std::adjacent_find(replicas.begin(), replicas.end()) != replicas.end())
        throw std::invalid_argument("duplicate replica");
    replicas.erase(std::find(replicas.begin(), replicas.end(), self));
    return replicas;
}
} // namespace

MuCompactLog::MuCompactLog(rdma::Transport& net, std::vector<NodeId> replicas,
                           size_t window, size_t capacity_limit,
                           size_t lane, size_t lane_count, MuRecoveryConfig recovery)
    : net_(net), followers_(follower_ids(net.self(), std::move(replicas), recovery.promoted)),
      ring_(window), layout_(recovery.enabled ? compact_recovery_capacity(net.memory_bytes(), recovery.handoff) :
                             log_capacity(net.memory_bytes(), capacity_limit), lane, lane_count),
      progress_(followers_.size(), recovery.recovered, recovery.promoted ? 1 : 0),
      lane_(lane), recovery_(recovery), applied_(recovery.recovered) {
    if ((recovery.enabled && capacity_limit) ||
        (recovery.promoted && !recovery.enabled) ||
        recovery.recovered > layout_.capacity())
        throw std::invalid_argument("invalid recovered compact log");
    if (!window || window > 4096)
        throw std::invalid_argument("invalid Mu log pipeline window");
    // All participants must use identical physical offsets. Do not allow
    // unequal MR sizes to create divergent lane partitions.
    for (NodeId peer : followers_)
        if (net_.remote_memory_bytes(peer) != net_.memory_bytes())
            throw std::invalid_argument("replicas have different log memory sizes");
}

size_t MuCompactLog::available() const {
    if (!progress_.can_post()) return 0;
    return std::min(max_batch, ring_.size() - inflight());
}

void MuCompactLog::append_batch(std::span<const Message> mutations) {
    if (mutations.empty() || mutations.size() > available())
        throw std::runtime_error("invalid compact log batch size");
    const uint64_t begin = progress_.posted();
    const uint64_t end = begin + mutations.size();
    const uint64_t first = layout_.global_slot(begin);
    (void)layout_.global_slot(end - 1); // Validate the whole range before posting.
    std::array<uint64_t, max_batch> words;
    for (size_t i = 0; i < mutations.size(); ++i)
        words[i] = pack_mu_command(mutations[i]);
    const size_t offset = rdma::kLogOffset + first * sizeof(uint64_t);
    auto* local = static_cast<uint64_t*>(net_.at(offset, mutations.size() * sizeof(uint64_t)));
    for (size_t i = 0; i < mutations.size(); ++i) {
        local[i] = words[i];
        ring_[(begin + i) % ring_.size()] = {begin + i, mutations[i]};
    }
    // Every batch is signaled on every follower. Its exclusive end is the
    // CQE token. RC ordering is per QP; never infer another lane's progress.
    const auto payload = std::span<const uint64_t>(words.data(), mutations.size());
    for (NodeId peer : followers_) {
        if (recovery_.enabled && recovery_.publication)
            net_.write_published_words(peer, payload, offset,
                compact_publication_offset(net_.memory_bytes(), first, recovery_.handoff), end, lane_);
        else net_.write_inline_words(peer, payload, offset, end, lane_);
    }
    progress_.posted(end);
}

void MuCompactLog::on_write_complete(NodeId peer, WorkId id) {
    const auto found = std::find(followers_.begin(), followers_.end(), peer);
    if (found == followers_.end())
        throw std::runtime_error("compact log completion from non-follower");
    progress_.completed(static_cast<size_t>(found - followers_.begin()), id);
}

std::optional<Committed> MuCompactLog::next_committed() {
    if (applied_ == progress_.committed()) return std::nullopt;
    auto& entry = ring_[applied_ % ring_.size()];
    if (entry.slot != applied_)
        throw std::runtime_error("compact log slot ordering violation");
    const uint64_t global = layout_.global_slot(applied_);
    ++applied_;
    return Committed{global, entry.command};
}
} // namespace lock
