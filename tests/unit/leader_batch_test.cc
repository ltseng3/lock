#include "lock/locks/leader_response_batch.h"
#include "lock/locks/mu_compact_log.h"
#include "lock/locks/handoff_layout.h"
#include <cassert>
#include <memory>
#include <stdexcept>
#include <vector>

namespace {
using namespace lock;
struct Write {
    NodeId peer;
    std::vector<uint64_t> words;
    size_t offset;
    WorkId end;
    size_t lane;
};
struct Send {
    NodeId peer;
    std::vector<Message> messages;
    WorkId end;
    size_t lane;
};
std::vector<Write> writes;
std::vector<Send> sends;
template <typename F> void rejected(F fn) {
    bool failed = false;
    try { fn(); } catch (const std::exception&) { failed = true; }
    assert(failed);
}
}

// Record real log/response engine calls without an RDMA adapter. Deliberately
// deliver completions in different orders across QPs, but FIFO within each QP.
namespace lock::rdma {
struct Transport::Impl {
    Config cfg;
    std::vector<uint64_t> words;
    explicit Impl(Config c) : cfg(std::move(c)),
        words((kLogOffset + 4096) / sizeof(uint64_t)) {}
};
Transport::Transport(Config c) : impl_(std::make_unique<Impl>(std::move(c))) {}
Transport::~Transport() = default;
void* Transport::at(size_t offset, size_t length) {
    const size_t bytes = impl_->words.size() * sizeof(uint64_t);
    if (offset > bytes || length > bytes - offset)
        throw std::out_of_range("fake log");
    return reinterpret_cast<char*>(impl_->words.data()) + offset;
}
size_t Transport::memory_bytes() const { return impl_->cfg.memory_bytes; }
size_t Transport::remote_memory_bytes(NodeId) const { return memory_bytes(); }
NodeId Transport::self() const { return impl_->cfg.self; }
void Transport::write_inline_words(NodeId peer, std::span<const uint64_t> words,
                                   size_t offset, WorkId end, size_t lane) {
    writes.push_back({peer, {words.begin(), words.end()}, offset, end, lane});
}
void Transport::send_batch(NodeId peer, std::span<const Message> messages,
                           WorkId end, size_t lane) {
    sends.push_back({peer, {messages.begin(), messages.end()}, end, lane});
}
} // namespace lock::rdma

namespace {
Message acquire(uint64_t sequence, LockId lock = 1) {
    return message(Kind::Acquire, 6, lock, make_request_id(6, sequence));
}
void test_progress() {
    ReplicationProgress p(4);
    p.posted(3); p.posted(8); p.posted(9);
    rejected([&] { p.completed(0, 8); }); // cannot skip an unobserved CQE
    p.completed(0, 3); p.completed(0, 8); p.completed(0, 9);
    assert(p.committed() == 0); // leader plus one follower not enough
    p.completed(1, 3);
    assert(p.committed() == 3);
    p.completed(2, 3); p.completed(2, 8);
    assert(p.committed() == 8);
    p.completed(2, 9);
    assert(p.committed() == 9);
    p.completed(3, 3);
    assert(p.committed() == 9);
    rejected([&] { p.completed(3, 3); });

    ReplicationProgress slow(2);
    for (uint64_t i = 1; i <= ReplicationProgress::max_outstanding_batches; ++i) {
        assert(slow.can_post());
        slow.posted(i);
        slow.completed(0, i);
        assert(slow.committed() == i);
    }
    assert(!slow.can_post()); // quorum progress does not recycle other SQs
    rejected([&] { slow.posted(513); });
    slow.completed(1, 1);
    assert(slow.can_post());
    slow.posted(513);
}
void test_log(rdma::Transport& net) {
    MuCompactLog log(net, {1,2,3}, 4, 128, 1, 2);
    assert(log.capacity() == 64 && log.available() == 4);
    const std::array first{acquire(1), acquire(2), acquire(3)};
    const std::array second{acquire(4)};
    log.append_batch(first);
    log.append_batch(second); // singleton immediately, no full-batch wait
    assert(writes.size() == 4 && log.available() == 0);
    assert(writes[0].words.size() == 3 && writes[0].end == 3);
    assert(writes[0].words == writes[1].words);
    assert(writes[0].offset == rdma::kLogOffset + 64 * 8);
    assert(writes[2].offset == writes[0].offset + 24 && writes[2].end == 4);
    assert(writes[2].lane == 1);
    assert(!log.next_committed());
    log.on_write_complete(3, 3);
    for (size_t i = 0; i < first.size(); ++i) {
        auto item = log.next_committed();
        assert(item && item->global_slot == 64 + i &&
               item->command.request == first[i].request);
        assert(unpack_mu_command(writes[0].words[i]).request == first[i].request);
    }
    assert(!log.next_committed());
    const std::array third{acquire(5), acquire(6), acquire(7)};
    log.append_batch(third); // ring wraps, physical log remains append-only
    assert(writes[4].offset == writes[0].offset + 32);
    log.on_write_complete(2, 3); // slow follower of already applied prefix
    assert(!log.next_committed());
    log.on_write_complete(2, 4);
    auto four = log.next_committed();
    assert(four && four->command.request == second[0].request);
    assert(!log.next_committed());
    log.on_write_complete(2, 7);
    for (auto& m : third) {
        auto item = log.next_committed();
        assert(item && item->command.request == m.request);
    }
    assert(!log.next_committed() && log.inflight() == 0);
    log.on_write_complete(3, 4); log.on_write_complete(3, 7);
    rejected([&] { log.on_write_complete(3, 7); });
    rejected([&] { log.on_write_complete(5, 7); });
}
void test_responses(rdma::Transport& net) {
    const std::vector<NodeId> clients{6,7};
    LeaderResponseBatch batch(net, 1, clients, 1, 2, 8);
    SpscQueue<Action, 1024> queue;
    auto add = [&](NodeId peer, Kind kind, uint64_t sequence) {
        bool ok = queue.try_push({peer, message(kind, 0, 1, make_request_id(peer, sequence))});
        assert(ok);
    };
    add(6, Kind::Next, 1); add(7, Kind::Order, 2); add(6, Kind::ReleaseAck, 1);
    assert(batch.drain(queue) == 3 && sends.size() == 2);
    assert(sends[0].peer == 6 && sends[0].end == 2 && sends[0].lane == 1);
    assert(message_kind(sends[0].messages[0]) == Kind::Next);
    assert(message_kind(sends[0].messages[1]) == Kind::ReleaseAck);
    assert(sends[1].messages.size() == 1 && sends[1].messages[0].sender == 1);
    batch.completed(7, 1); batch.completed(6, 2);
    rejected([&] { batch.completed(6, 2); });
    for (size_t i = 0; i < LeaderResponseBatch::sq_credit; ++i)
        add(6, Kind::Order, i + 2);
    for (size_t i = 0; i < LeaderResponseBatch::sq_credit; i += 64)
        assert(batch.drain(queue) == 64);
    add(6, Kind::Order, 1000); add(7, Kind::Order, 1001);
    assert(batch.drain(queue) == 1); // peer 6 credit exhausted; peer 7 proceeds
    assert(batch.drain(queue) == 0); // staged suffix retained
    batch.completed(6, 10);
    assert(batch.drain(queue) == 1);
    assert(sends.back().messages[0].request == make_request_id(6, 1000));
    assert(queue.size() == 0);
}
}
int main() {
    lock::rdma::Config cfg; cfg.self = 1;
    lock::rdma::Transport net(cfg);
    test_progress();
    test_log(net);
    test_responses(net);
}
