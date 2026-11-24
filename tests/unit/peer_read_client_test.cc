#include "lock/bench/client.h"
#include "lock/locks/handoff_state.h"
#include <cassert>
#include <algorithm>
#include <memory>
#include <stdexcept>
#include <vector>

// A deterministic fake transport. Drive the production ClientEngine with
// explicitly ordered events; this does not simulate adapter ordering or DMA.
namespace {
using namespace lock;
struct Posted {
    rdma::CompletionKind kind;
    NodeId peer;
    WorkId id;
    uint64_t* result;
    Message message;
};
std::vector<Posted> posted;
}
namespace lock::rdma {
struct Transport::Impl {
    Config cfg;
    std::vector<uint64_t> words;
    size_t next;
    explicit Impl(Config c) : cfg(std::move(c)),
        words(cfg.memory_bytes / 8), next(cfg.memory_bytes - 16ULL * 1024 * 1024) {}
};
Transport::Transport(Config c) : impl_(std::make_unique<Impl>(std::move(c))) {}
Transport::~Transport() = default;
void* Transport::at(size_t offset, size_t length) {
    if (offset > memory_bytes() || length > memory_bytes() - offset)
        throw std::out_of_range("fake MR");
    return reinterpret_cast<char*>(impl_->words.data()) + offset;
}
void* Transport::allocate(size_t length, size_t alignment) {
    impl_->next = (impl_->next + alignment - 1) & ~(alignment - 1);
    void* ptr = at(impl_->next, length);
    impl_->next += length;
    return ptr;
}
size_t Transport::memory_bytes() const { return impl_->cfg.memory_bytes; }
size_t Transport::remote_memory_bytes(NodeId) const { return memory_bytes(); }
void Transport::send(NodeId peer, const Message& m, WorkId id, bool, size_t) {
    posted.push_back({CompletionKind::Send, peer, id, nullptr, m});
}
bool Transport::write(NodeId peer, const void*, size_t, size_t, WorkId id, bool signal) {
    posted.push_back({CompletionKind::Write, peer, id, nullptr, {}});
    return signal;
}
void Transport::read_word(NodeId peer, uint64_t* result, size_t, WorkId id) {
    posted.push_back({CompletionKind::Read, peer, id, result, {}});
}
void Transport::compare_swap(NodeId, uint64_t*, size_t, uint64_t, uint64_t, WorkId) {
    throw std::runtime_error("unexpected fake CAS");
}
void Transport::poll_into(std::vector<Completion>& out, int) { out.clear(); }
} // namespace lock::rdma

namespace lock {
struct ClientEngineTestAccess {
    static rdma::Completion receive(Message m) {
        rdma::Completion c{};
        c.kind = rdma::CompletionKind::Receive;
        c.peer = m.sender;
        c.message = m;
        return c;
    }
    static rdma::Completion done(Posted p) {
        rdma::Completion c{};
        c.kind = p.kind; c.peer = p.peer; c.work_id = p.id;
        return c;
    }
    static Posted last(rdma::CompletionKind kind) {
        auto it = std::find_if(posted.rbegin(), posted.rend(),
                               [&](const Posted& p) { return p.kind == kind; });
        if (it == posted.rend()) throw std::runtime_error("missing posted operation");
        return *it;
    }
    static size_t writes() {
        return std::count_if(posted.begin(), posted.end(), [](const Posted& p) {
            return p.kind == rdma::CompletionKind::Write;
        });
    }
    static void run() {
        rdma::Config nc; nc.self = 5;
        rdma::Transport net(nc);
        ClientConfig cfg;
        cfg.self = 5; cfg.replicas = {1,2,3}; cfg.backend = Backend::Handoff;
        cfg.handoff_peer_read = true; cfg.lock_count = 1;
        ClientEngine e(net, cfg);
        const auto a = make_request_id(4, 9);
        auto begin = [&](RequestId predecessor, uint64_t pos) {
            posted.clear();
            e.submit(0);
            const auto request = e.ops_[0].request;
            auto order = message(Kind::Order, 1, 0, request);
            order.other = predecessor; order.position = pos; order.value = pos;
            e.handle(receive(order));
            return request;
        };
        auto handoff = [&](RequestId request, RequestId predecessor, uint64_t pos) {
            auto m = message(Kind::Handoff, request_node(predecessor), 0, request);
            m.other = predecessor; m.position = pos;
            return m;
        };
        auto finish = [&](RequestId request, uint64_t pos) {
            e.handle(done(last(rdma::CompletionKind::Write))); // protected WRITE
            e.handle(done(last(rdma::CompletionKind::Write))); // marker WRITE
            auto ack = message(Kind::ReleaseAck, 1, 0, request);
            ack.position = pos; e.handle(receive(ack));
            assert(e.retired_.empty());
            assert(e.requests_.empty());
            assert(*static_cast<uint64_t*>(net.at(peer_completion_offset(request), 8))
                   == request);
        };
        // READ first; even after operation completion and reuse, the old
        // valid HANDOFF must be consumed without touching the new request.
        auto b = begin(a, 1);
        auto read = last(rdma::CompletionKind::Read);
        *read.result = a;
        e.handle(done(read));
        assert(writes() == 1 && e.activation_by_read_ == 1);
        finish(b, 1);
        auto c = begin(a, 2);
        assert(e.read_receipts_.size() == 1);
        auto invalid = handoff(b, a, 1); ++invalid.position;
        bool threw = false;
        try { e.handle(receive(invalid)); } catch (const std::runtime_error&) { threw = true; }
        assert(threw && e.read_receipts_.size() == 1 && writes() == 0);
        e.handle(receive(handoff(b, a, 1)));
        assert(writes() == 0 && e.read_receipts_.empty() && e.redundant_handoffs_ == 1);
        // HANDOFF first, including completion of the protected operation
        // before the outstanding READ returns. Buffer remains pinned.
        read = last(rdma::CompletionKind::Read);
        e.handle(receive(handoff(c, a, 2)));
        assert(writes() == 1);
        finish(c, 2);
        assert(e.ops_[0].read_pending);
        threw = false;
        try { e.submit(0); } catch (const std::logic_error&) { threw = true; }
        assert(threw);
        *read.result = a; e.handle(done(read));
        assert(!e.ops_[0].read_pending && writes() == 2);
        // A ring collision / unrelated generation is never a release proof.
        auto d = begin(a, 3);
        read = last(rdma::CompletionKind::Read);
        const auto collision = make_request_id(4, 9 + kPeerCompletionSlots);
        assert(peer_completion_offset(collision) == peer_completion_offset(a));
        *read.result = collision; e.handle(done(read));
        assert(writes() == 0 && e.read_misses_ == 1);
        e.handle(receive(handoff(d, a, 3))); finish(d, 3);
        // Same-client predecessor can be checked locally.
        auto local = begin(d, 4);
        assert(e.local_probes_ == 1 && writes() == 1);
        finish(local, 4); e.handle(receive(handoff(local, d, 4)));
        assert(e.read_receipts_.empty() && e.awaited_.empty());
        // Handoff already arrived before ORDER: do not issue a READ.
        posted.clear(); e.submit(0); auto early = e.ops_[0].request;
        e.handle(receive(handoff(early, a, 5)));
        auto order = message(Kind::Order, 1, 0, early);
        order.other = a; order.position = 5; order.value = 5;
        e.handle(receive(order));
        assert(writes() == 1 && !e.ops_[0].read_pending);
        finish(early, 5);
        // Leader ordering invariant for ACK reclamation and receipt drain:
        // if A is named as predecessor, NEXT precedes ACK for A.
        HandoffState state(1, 2);
        auto ma = message(Kind::Acquire, 4, 0, a);
        auto mb = message(Kind::Acquire, 5, 0, b);
        state.admit(ma); state.committed({0, ma}); state.admit(mb);
        auto ordered = state.committed({1, mb});
        assert(ordered.size() == 2 && message_kind(ordered[1].message) == Kind::Next);
        auto retired = state.retire([&](uint64_t slot) { return slot == 0 ? a : 0; });
        assert(retired.actions.size() == 1);
        assert(message_kind(retired.actions[0].message) == Kind::ReleaseAck);
        assert(retired.actions[0].message.request == ordered[1].message.request);
    }
};
}
int main() { lock::ClientEngineTestAccess::run(); }
