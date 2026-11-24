#include "lock/bench/client.h"
#include "lock/locks/handoff_state.h"
#include "lock/locks/handoff_layout.h"
#include "lock/locks/retirement_gc.h"
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
    size_t offset = 0;
    uint64_t inline_value = 0;
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
bool Transport::write(NodeId peer, const void*, size_t, size_t offset, WorkId id, bool signal) {
    posted.push_back({CompletionKind::Write, peer, id, nullptr, {}, offset});
    return signal;
}
bool Transport::write_inline(NodeId peer, uint64_t value, size_t offset,
                             WorkId id, bool signal, size_t lane) {
    if (!signal || lane != 0) throw std::runtime_error("unexpected inline marker mode");
    posted.push_back({CompletionKind::Write, peer, id, nullptr, {}, offset, value});
    return signal;
}
void Transport::read_word(NodeId peer, uint64_t* result, size_t, WorkId id) {
    posted.push_back({CompletionKind::Read, peer, id, result, {}});
}
void Transport::compare_swap(NodeId peer, uint64_t* result, size_t offset,
                             uint64_t, uint64_t, WorkId id) {
    posted.push_back({CompletionKind::CompareSwap, peer, id, result, {}, offset});
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
    static void require(bool ok) {
        if (!ok) throw std::runtime_error("empty critical section regression failed");
    }
    static void test_empty_sections() {
        rdma::Config nc; nc.self = 5;
        rdma::Transport net(nc);
        ClientConfig cfg;
        cfg.self = 5; cfg.replicas = {1,2,3};
        cfg.critical_section_ops = 0; cfg.lock_count = 1;
        // CAS still waits for the actual unlock CQE; no data WRITE.
        {
            cfg.backend = Backend::Cas;
            ClientEngine e(net, cfg);
            posted.clear(); e.submit(0);
            auto cas = last(rdma::CompletionKind::CompareSwap);
            *cas.result = 0; e.handle(done(cas));
            require(writes() == 0);
            e.finish_empty_sections();
            require(writes() == 1);
            auto unlock = last(rdma::CompletionKind::Write);
            require(unlock.offset == rdma::kFlagOffset && !e.requests_.empty());
            e.handle(done(unlock));
            require(e.requests_.empty() && e.awaited_.empty());
        }
        // Mu still requires its replicated RELEASE and server ACK.
        {
            cfg.backend = Backend::Mu;
            ClientEngine e(net, cfg);
            posted.clear(); e.submit(0);
            const auto request = e.ops_[0].request;
            e.handle(receive(message(Kind::Grant, 1, 0, request)));
            e.finish_empty_sections();
            require(writes() == 0 && !e.requests_.empty());
            require(message_kind(last(rdma::CompletionKind::Send).message) == Kind::Release);
            e.handle(receive(message(Kind::ReleaseAck, 1, 0, request)));
            require(e.requests_.empty());
        }
        // A prepared same-client successor chain must drain iteratively,
        // never re-enter a releasing predecessor or issue a protected WRITE.
        {
            cfg.backend = Backend::Handoff; cfg.outstanding = 3;
            cfg.handoff_peer_read = false;
            ClientEngine e(net, cfg);
            posted.clear();
            std::array<RequestId, 3> requests{};
            const RequestId predecessor = make_request_id(4, 99);
            for (size_t i = 0; i < requests.size(); ++i) {
                e.submit(i); requests[i] = e.ops_[i].request;
                auto order = message(Kind::Order, 1, 0, requests[i]);
                order.other = i ? requests[i-1] : predecessor;
                order.position = i + 1; order.value = i + 1;
                e.handle(receive(order));
            }
            for (size_t i = 0; i + 1 < requests.size(); ++i) {
                auto next = message(Kind::Next, 1, 0, requests[i]);
                next.other = requests[i+1]; next.position = i+1; next.value = i+2;
                e.handle(receive(next));
            }
            auto first = message(Kind::Handoff, 4, 0, requests[0]);
            first.other = predecessor; first.position = 1;
            e.handle(receive(first));
            require(writes() == 0 && e.empty_sections_.size() == 1);
            e.finish_empty_sections();
            require(e.empty_sections_.empty() && e.requests_.empty());
            require(writes() == 3 && e.handoffs_sent_ == 2 && e.handoffs_received_ == 3);
            size_t markers = 0;
            for (const auto& p : posted) {
                if (p.kind != rdma::CompletionKind::Write) continue;
                ++markers;
                require(p.offset == handoff_marker_offset(net.memory_bytes(), markers));
                require(p.inline_value == requests[markers-1]);
                e.handle(done(p));
            }
            require(markers == 3 && e.markers_drained());
            for (size_t i = 0; i < requests.size(); ++i) {
                auto ack = message(Kind::ReleaseAck, 1, 0, requests[i]);
                ack.position = i+1; e.handle(receive(ack));
            }
            require(e.retired_.empty() && e.awaited_.empty());
        }
        // Zero-work peer READ winner publishes a proof and still consumes
        // the redundant normal HANDOFF after local retirement.
        {
            cfg.outstanding = 1; cfg.handoff_peer_read = true;
            ClientEngine e(net, cfg);
            posted.clear(); e.submit(0);
            const auto request = e.ops_[0].request;
            const auto predecessor = make_request_id(4, 100);
            auto order = message(Kind::Order, 1, 0, request);
            order.other = predecessor; order.position = 1; order.value = 1;
            e.handle(receive(order));
            auto read = last(rdma::CompletionKind::Read);
            *read.result = predecessor; e.handle(done(read));
            require(writes() == 0);
            e.finish_empty_sections();
            require(writes() == 1 && e.read_receipts_.size() == 1);
            require(*static_cast<uint64_t*>(net.at(peer_completion_offset(request), 8)) == request);
            e.handle(done(last(rdma::CompletionKind::Write)));
            auto late = message(Kind::Handoff, 4, 0, request);
            late.other = predecessor; late.position = 1; e.handle(receive(late));
            auto ack = message(Kind::ReleaseAck, 1, 0, request);
            ack.position = 1; e.handle(receive(ack));
            require(e.read_receipts_.empty() && e.retired_.empty() &&
                    e.requests_.empty() && e.awaited_.empty());
        }
    }
    static void test_inline_marker_backpressure() {
        rdma::Config nc; nc.self = 5;
        rdma::Transport net(nc);
        ClientConfig cfg;
        cfg.self = 5; cfg.replicas = {1,2,3}; cfg.backend = Backend::Handoff;
        cfg.critical_section_ops = 0; cfg.lock_count = 1;
        ClientEngine e(net, cfg);
        posted.clear();
        const size_t budget = e.marker_credit_;
        std::vector<RequestId> ids;
        std::vector<Posted> markers;
        for (size_t i = 0; i <= budget; ++i) {
            e.submit(0);
            ids.push_back(e.ops_[0].request);
            auto order = message(Kind::Order, 1, 0, ids.back());
            order.position = i+1; order.value = i;
            e.handle(receive(order));
            e.finish_empty_sections();
            if (i < budget) {
                auto p = last(rdma::CompletionKind::Write);
                require(p.inline_value == ids.back());
                markers.push_back(p);
            }
        }
        require(writes() == budget && e.deferred_marker_slots_.size() == 1);
        require(e.requests_.size() == 1 && !e.markers_drained());
        bool threw = false;
        try { e.handle(done(markers[1])); } catch (const std::runtime_error&) { threw = true; }
        require(threw && e.markers_completed_ == 0);
        e.handle(done(markers.front()));
        require(writes() == budget + 1 && e.deferred_marker_slots_.empty());
        require(e.requests_.empty());
        markers.push_back(last(rdma::CompletionKind::Write));
        require(markers.back().inline_value == ids.back());
        threw = false;
        try { e.handle(done(markers.front())); } catch (const std::runtime_error&) { threw = true; }
        require(threw);
        for (size_t i = 1; i < markers.size(); ++i) e.handle(done(markers[i]));
        require(e.markers_drained());
        for (size_t i = 0; i < ids.size(); ++i) {
            auto ack = message(Kind::ReleaseAck, 1, 0, ids[i]);
            ack.position = i+1; e.handle(receive(ack));
        }
        require(e.retired_.empty() && e.awaited_.empty());
    }
    static void test_retirement_gc() {
        rdma::Config nc; nc.self = 5;
        rdma::Transport net(nc);
        ClientConfig cfg;
        cfg.self = 5; cfg.replicas = {1,2,3}; cfg.backend = Backend::Handoff;
        cfg.critical_section_ops = 0; cfg.lock_count = 1; cfg.handoff_gc = true;
        ClientEngine e(net, cfg);
        posted.clear();
        auto order = [&](uint64_t pos) {
            e.submit(0);
            const auto id = e.ops_[0].request;
            auto m = message(Kind::Order, 1, 0, id);
            m.position = pos; m.value = pos;
            e.handle(receive(m));
            return id;
        };
        auto next = [&](RequestId id, uint64_t pos, RequestId successor) {
            auto m = message(Kind::Next, 1, 0, id);
            m.other = successor; m.position = pos; m.value = pos+1;
            e.handle(receive(m));
        };
        const auto successor = make_request_id(4, 100);
        const auto a = order(1);
        next(a, 1, successor);
        e.finish_empty_sections();
        require(e.retired_.empty() && e.gc_next_reclaimed_ == 1);
        e.handle(done(last(rdma::CompletionKind::Write)));
        const auto b = order(2);
        e.finish_empty_sections();
        require(e.retired_.contains(b));
        e.handle(done(last(rdma::CompletionKind::Write)));
        next(b, 2, successor);
        require(e.retired_.empty() && e.gc_next_reclaimed_ == 2);
        const auto x = order(3);
        e.finish_empty_sections(); e.handle(done(last(rdma::CompletionKind::Write)));
        const auto y = order(4);
        e.finish_empty_sections(); e.handle(done(last(rdma::CompletionKind::Write)));
        require(e.retired_.size() == 2);
        const std::array bad{x, b};
        bool threw = false;
        try { e.handle(receive(retirement_gc_message(1, 0, bad))); }
        catch (const std::runtime_error&) { threw = true; }
        require(threw && e.retired_.size() == 2);
        const std::array valid{x,y};
        auto packet = receive(retirement_gc_message(1, 0, valid));
        packet.lane = 1;
        threw = false;
        try { e.handle(packet); } catch (const std::runtime_error&) { threw = true; }
        require(threw && e.retired_.size() == 2);
        packet.lane = 0; e.handle(packet);
        require(e.retired_.empty() && e.gc_ack_records_ == 2 && e.gc_ack_messages_ == 1);
        threw = false;
        try { e.handle(packet); } catch (const std::runtime_error&) { threw = true; }
        require(threw);
        require(e.markers_drained() && e.requests_.empty() && e.awaited_.empty());

        HandoffState state(1, 2, true);
        auto ma = message(Kind::Acquire, 5, 0, a);
        auto mb = message(Kind::Acquire, 5, 0, b);
        state.admit(ma); state.committed({0, ma});
        state.admit(mb);
        auto actions = state.committed({1, mb});
        require(actions.size() == 2 && message_kind(actions[1].message) == Kind::Next);
        require(state.retire([&](uint64_t i) { return i == 0 ? a : 0; }).actions.empty());
        auto tail = state.retire([&](uint64_t i) { return i == 1 ? b : 0; });
        require(tail.actions.size() == 1 && tail.actions[0].message.request == b);
        auto mc = message(Kind::Acquire, 5, 0, x);
        state.admit(mc);
        auto after = state.committed({2, mc});
        require(after.size() == 1 && after[0].message.other == 0);
    }
    static void run() {
        test_empty_sections();
        test_retirement_gc();
        test_inline_marker_backpressure();
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
