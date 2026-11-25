#include "lock/locks/leader_response_batch.h"
#include "lock/locks/mu_compact_log.h"
#include "lock/locks/handoff_layout.h"
#include <array>
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
    size_t marker_offset = 0;
};
struct Send {
    NodeId peer;
    std::vector<Message> messages;
    WorkId end;
    size_t lane;
};
std::vector<Write> writes;
std::vector<Send> sends;
NodeId fail_send_peer = 0;
void require(bool condition) {
    if (!condition) throw std::runtime_error("leader batch regression failed");
}
template <typename F> void rejected(F fn) {
    bool failed = false;
    try { fn(); } catch (const std::exception&) { failed = true; }
    require(failed);
}
}

// Record real log/response engine calls without an RDMA adapter. Deliberately
// deliver completions in different orders across QPs, but FIFO within each QP.
namespace lock::rdma {
struct Transport::Impl {
    Config cfg;
    std::vector<uint64_t> words;
    explicit Impl(Config c) : cfg(std::move(c)),
        words((cfg.memory_bytes < kDefaultMemoryBytes ? cfg.memory_bytes :
               kLogOffset + 4096) / sizeof(uint64_t)) {}
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
void Transport::write_published_words(NodeId peer, std::span<const uint64_t> words,
                                     size_t offset, size_t marker_offset, WorkId end, size_t lane) {
    writes.push_back({peer, {words.begin(), words.end()}, offset, end, lane, marker_offset});
}
void Transport::send_batch(NodeId peer, std::span<const Message> messages,
                           WorkId end, size_t lane) {
    if (peer == fail_send_peer) throw PeerUnavailable(peer);
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
    require(p.committed() == 0); // leader plus one follower not enough
    p.completed(1, 3);
    require(p.committed() == 3);
    p.completed(2, 3); p.completed(2, 8);
    require(p.committed() == 8);
    p.completed(2, 9);
    require(p.committed() == 9);
    p.completed(3, 3);
    require(p.committed() == 9);
    rejected([&] { p.completed(3, 3); });

    ReplicationProgress recovered(1, 17, 1);
    require(recovered.committed() == 17 && recovered.posted() == 17);
    recovered.posted(20);
    require(recovered.committed() == 17);
    recovered.completed(0, 20);
    require(recovered.committed() == 20);
    rejected([&] { recovered.completed(0, 20); });

    ReplicationProgress slow(2);
    for (uint64_t i = 1; i <= ReplicationProgress::max_outstanding_batches; ++i) {
        require(slow.can_post());
        slow.posted(i);
        slow.completed(0, i);
        require(slow.committed() == i);
    }
    require(!slow.can_post()); // quorum progress does not recycle other SQs
    rejected([&] { slow.posted(513); });
    slow.completed(1, 1);
    require(slow.can_post());
    slow.posted(513);
}
void test_log(rdma::Transport& net) {
    MuCompactLog log(net, {1,2,3}, 4, 128, 1, 2);
    require(log.capacity() == 64 && log.available() == 4);
    const std::array first{acquire(1), acquire(2), acquire(3)};
    const std::array second{acquire(4)};
    log.append_batch(first);
    log.append_batch(second); // singleton immediately, no full-batch wait
    require(writes.size() == 4 && log.available() == 0);
    require(writes[0].words.size() == 3 && writes[0].end == 3);
    require(writes[0].words == writes[1].words);
    require(writes[0].offset == rdma::kLogOffset + 64 * 8);
    require(writes[2].offset == writes[0].offset + 24 && writes[2].end == 4);
    require(writes[2].lane == 1);
    require(!log.next_committed());
    log.on_write_complete(3, 3);
    for (size_t i = 0; i < first.size(); ++i) {
        auto item = log.next_committed();
        require(item && item->global_slot == 64 + i &&
               item->command.request == first[i].request);
        require(unpack_mu_command(writes[0].words[i]).request == first[i].request);
    }
    require(!log.next_committed());
    const std::array third{acquire(5), acquire(6), acquire(7)};
    log.append_batch(third); // ring wraps, physical log remains append-only
    require(writes[4].offset == writes[0].offset + 32);
    log.on_write_complete(2, 3); // slow follower of already applied prefix
    require(!log.next_committed());
    log.on_write_complete(2, 4);
    auto four = log.next_committed();
    require(four && four->command.request == second[0].request);
    require(!log.next_committed());
    log.on_write_complete(2, 7);
    for (auto& m : third) {
        auto item = log.next_committed();
        require(item && item->command.request == m.request);
    }
    require(!log.next_committed() && log.inflight() == 0);
    log.on_write_complete(3, 4); log.on_write_complete(3, 7);
    rejected([&] { log.on_write_complete(3, 7); });
    rejected([&] { log.on_write_complete(5, 7); });
}
void test_recovered_log() {
    rdma::Config cfg; cfg.self = 2;
    // Small fake MR: enough room for 128 words and their byte sidecar.
    cfg.memory_bytes = rdma::kLogOffset + 16ULL * 1024 * 1024 + 128 * 9;
    rdma::Transport net(cfg);
    MuLogView view(net, true);
    require(view.capacity() == 128);
    auto* data = view.words(0, 8);
    auto* marks = view.marks(0, 8);
    for (uint64_t i = 0; i < 5; ++i) data[i] = pack_mu_command(acquire(i+1));
    marks[0] = marks[1] = marks[2] = marks[4] = 1;
    require(view.scan(0) == 3); // a later nonzero word cannot fill a hole
    require(view.command(2).request == make_request_id(6, 3));
    marks[3] = 1;
    require(view.scan(3) == 5);
    // A payload with no publication is not recoverable, even if nonzero.
    data[5] = pack_mu_command(acquire(6));
    require(view.scan(5) == 5);
    MuLogView raw(net, false);
    require(raw.scan(0) == 6);
    MuRecoveryConfig cfg_recovery;
    cfg_recovery.enabled = cfg_recovery.promoted = true;
    cfg_recovery.recovered = 5;
    MuCompactLog log(net, {2,3}, 2, 0, 0, 1, cfg_recovery);
    require(log.inflight() == 0 && !log.next_committed());
    writes.clear();
    const std::array next{acquire(6), acquire(7)};
    log.append_batch(next);
    require(writes.size() == 1 && writes[0].peer == 3 && writes[0].end == 7);
    require(writes[0].offset == rdma::kLogOffset + 5*8);
    require(writes[0].marker_offset == mu_recovery_marker_offset(cfg.memory_bytes, 5));
    require(log.available() == 0 && !log.next_committed());
    log.on_write_complete(3, 7);
    auto a = log.next_committed();
    require(a && a->global_slot == 5 && a->command.request == next[0].request);
    auto b = log.next_committed();
    require(b && b->global_slot == 6 && b->command.request == next[1].request);
    require(!log.next_committed() && log.available() == 2);
    writes.clear();
}
void test_gc_responses(rdma::Transport& net) {
    sends.clear();
    LeaderResponseBatch batch(net, 1, {6,7}, 1, 2, 8, true);
    SpscQueue<Action, 1024> queue;
    auto add = [&](NodeId peer, Kind kind, uint64_t seq) {
        require(queue.try_push({peer, message(kind, 0, 1, make_request_id(peer, seq))}));
    };
    add(6, Kind::Order, 10);
    for (uint64_t i = 1; i <= 6; ++i) add(6, Kind::ReleaseAck, i);
    add(7, Kind::ReleaseAck, 11);
    add(6, Kind::Next, 12);
    require(batch.drain(queue) == 5 && queue.size() == 0);
    require(sends.size() == 2 && sends[0].messages.size() == 4);
    const auto& ms = sends[0].messages;
    require(message_kind(ms[0]) == Kind::Order);
    auto ids = decode_retirement_gc(ms[1], 6, 1);
    require(ms[1].flags == 5 && ids[4] == make_request_id(6, 5));
    require(message_kind(ms[2]) == Kind::Next);
    ids = decode_retirement_gc(ms[3], 6, 1);
    require(ms[3].flags == 1 && ids[0] == make_request_id(6, 6));
    require(decode_retirement_gc(sends[1].messages[0], 7, 1)[0] == make_request_id(7, 11));
    batch.completed(6, 4); batch.completed(7, 1);
    sends.clear();
}
void test_failed_post(rdma::Transport& net) {
    sends.clear();
    LeaderResponseBatch batch(net, 1, {6,7}, 1, 2, 8);
    SpscQueue<Action, 1024> queue;
    require(queue.try_push({6, message(Kind::Order, 0, 1, make_request_id(6, 1))}));
    require(queue.try_push({7, message(Kind::Order, 0, 1, make_request_id(7, 1))}));
    fail_send_peer = 6;
    require(batch.drain(queue) == 1 && queue.size() == 0);
    fail_send_peer = 0;
    require(sends.size() == 1 && sends[0].peer == 7);
    batch.completed(7, 1);
    batch.isolate(6); // subsequent transport event is idempotent
    require(queue.try_push({6, message(Kind::Order, 0, 1, make_request_id(6, 2))}));
    require(queue.try_push({7, message(Kind::Order, 0, 1, make_request_id(7, 2))}));
    require(batch.drain(queue) == 1 && sends.back().peer == 7);
    batch.completed(7, 2);
    sends.clear();
}

void test_isolated_responses(rdma::Transport& net) {
    sends.clear();
    LeaderResponseBatch batch(net, 1, {6,7}, 1, 2, 8);
    SpscQueue<Action, 1024> queue;
    auto add = [&](NodeId peer, uint64_t seq) {
        require(queue.try_push({peer, message(Kind::Order, 0, 1,
                                             make_request_id(peer, seq))}));
    };
    for (size_t i = 0; i < LeaderResponseBatch::sq_credit; ++i)
        add(6, i + 1);
    for (size_t i = 0; i < LeaderResponseBatch::sq_credit; i += 64)
        require(batch.drain(queue) == 64);
    add(6, 1000);
    add(7, 1);
    require(batch.drain(queue) == 1);
    require(batch.drain(queue) == 0); // blocked suffix for peer 6
    batch.isolate(6);
    batch.isolate(6); // duplicate notification is harmless
    batch.completed(6, 8); // trailing CQE cannot restore abandoned credits
    batch.completed(7, 1);
    add(6, 1001);
    add(7, 2);
    const size_t before = sends.size();
    require(batch.drain(queue) == 1 && queue.size() == 0);
    require(sends.size() == before + 1 && sends.back().peer == 7);
    require(sends.back().messages[0].request == make_request_id(7, 2));
    batch.completed(7, 2);
    rejected([&] { batch.completed(7, 2); }); // healthy accounting stays strict
    rejected([&] { batch.isolate(8); });
    sends.clear();
}

void test_responses(rdma::Transport& net) {
    const std::vector<NodeId> clients{6,7};
    LeaderResponseBatch batch(net, 1, clients, 1, 2, 8);
    SpscQueue<Action, 1024> queue;
    auto add = [&](NodeId peer, Kind kind, uint64_t sequence) {
        bool ok = queue.try_push({peer, message(kind, 0, 1, make_request_id(peer, sequence))});
        require(ok);
    };
    add(6, Kind::Next, 1); add(7, Kind::Order, 2); add(6, Kind::ReleaseAck, 1);
    require(batch.drain(queue) == 3 && sends.size() == 2);
    require(sends[0].peer == 6 && sends[0].end == 2 && sends[0].lane == 1);
    require(message_kind(sends[0].messages[0]) == Kind::Next);
    require(message_kind(sends[0].messages[1]) == Kind::ReleaseAck);
    require(sends[1].messages.size() == 1 && sends[1].messages[0].sender == 1);
    batch.completed(7, 1); batch.completed(6, 2);
    rejected([&] { batch.completed(6, 2); });
    for (size_t i = 0; i < LeaderResponseBatch::sq_credit; ++i)
        add(6, Kind::Order, i + 2);
    for (size_t i = 0; i < LeaderResponseBatch::sq_credit; i += 64)
        require(batch.drain(queue) == 64);
    add(6, Kind::Order, 1000); add(7, Kind::Order, 1001);
    require(batch.drain(queue) == 1); // peer 6 credit exhausted; peer 7 proceeds
    require(batch.drain(queue) == 0); // staged suffix retained
    batch.completed(6, 10);
    require(batch.drain(queue) == 1);
    require(sends.back().messages[0].request == make_request_id(6, 1000));
    require(queue.size() == 0);
}
}
int main() {
    lock::rdma::Config cfg; cfg.self = 1;
    lock::rdma::Transport net(cfg);
    test_progress();
    test_log(net);
    test_recovered_log();
    test_gc_responses(net);
    test_responses(net);
    test_isolated_responses(net);
    test_failed_post(net);
}
