#include "lock/bench/client.h"
#include <algorithm>
#include <atomic>
#include "lock/locks/replicated_log.h"
#include "lock/locks/handoff_layout.h"
#include "lock/locks/lane_layout.h"
#include <cstddef>
#include <chrono>
#include <cmath>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <thread>
namespace lock {
namespace {
uint64_t nanos(std::chrono::steady_clock::duration d) {
    return std::chrono::duration_cast<std::chrono::nanoseconds>(d).count();
}
uint64_t percentile(std::vector<uint64_t> values, double p) {
    if (values.empty()) return 0;
    std::sort(values.begin(), values.end());
    const size_t index = std::min(values.size() - 1,
                                 static_cast<size_t>(std::ceil(p * values.size())) - 1);
    return values[index];
}
} // namespace
ClientEngine::ClientEngine(rdma::Transport& net, ClientConfig cfg)
    : net_(net), cfg_(std::move(cfg)),
      ops_(cfg_.outstanding * cfg_.workers) {
    if (cfg_.workers < 1 || cfg_.workers > 16 ||
        cfg_.outstanding * cfg_.workers > 512)
        throw std::invalid_argument("workers * outstanding must be 1..512");
    if (cfg_.critical_section_ops > 1)
        throw std::invalid_argument("critical-section-ops must be 0 or 1");
    if (cfg_.critical_section_ops == 0) empty_sections_.reserve(ops_.size());
    pickers_.reserve(cfg_.workers);
    for (size_t worker = 0; worker < cfg_.workers; ++worker) {
        pickers_.emplace_back(cfg_.lock_count, cfg_.distribution,
            cfg_.seed ^ (static_cast<uint64_t>(cfg_.self) << 32) ^
            (worker * 0x9e3779b97f4a7c15ULL));
    }
    if (cfg_.replicas.size() != 3 && cfg_.replicas.size() != 5)
        throw std::invalid_argument("three or five replicas required");
    if (cfg_.mu_lanes == 0 || cfg_.mu_lanes > 8)
        throw std::invalid_argument("invalid client Mu lane count");
    if (cfg_.outstanding == 0 || cfg_.outstanding > 512 || cfg_.lock_count == 0 ||
        cfg_.lock_count > 1000)
        throw std::invalid_argument("invalid client window or lock count");
    if (cfg_.handoff_peer_read) {
        if (cfg_.backend != Backend::Handoff ||
            net_.memory_bytes() < rdma::kScratchOffset + 16ULL * 1024 * 1024)
            throw std::invalid_argument("peer READ requires the handoff client layout");
    }
    for (auto& op : ops_) {
        op.cas_result = static_cast<uint64_t*>(net_.allocate(sizeof(uint64_t), 8));
        op.data_value = static_cast<uint64_t*>(net_.allocate(sizeof(uint64_t), 8));
        op.release_zero = static_cast<uint64_t*>(net_.allocate(sizeof(uint64_t), 8));
        *op.release_zero = 0;
        if (cfg_.handoff_peer_read)
            op.read_result = static_cast<uint64_t*>(net_.allocate(sizeof(uint64_t), 8));
    }
    if (cfg_.backend == Backend::Handoff) {
        // Retain the existing marker WR budget; only remove buffer ownership.
        marker_credit_ = std::max<size_t>(
            128, std::min<size_t>(512, ops_.size() * 2));
    }
}
WorkId ClientEngine::track(size_t slot, Purpose purpose) {
    if (next_work_ >= kMarkerTokenBase)
        throw std::runtime_error("client work ID exhausted");
    WorkId id = next_work_++;
    awaited_.emplace(id, Awaited{slot, ops_.at(slot).generation, purpose});
    return id;
}
void ClientEngine::post_cas(size_t slot) {
    auto& op = ops_[slot];
    op.phase = Phase::WaitCas;
    NodeId owner = cfg_.replicas[op.lock % cfg_.replicas.size()];
    net_.compare_swap(owner, op.cas_result,
                      rdma::kFlagOffset + op.lock * sizeof(uint64_t),
                      0, 1, track(slot, Purpose::AcquireCas));
}
void ClientEngine::submit(size_t slot) {
    auto& op = ops_.at(slot);
    if (op.phase != Phase::Idle || op.read_pending)
        throw std::logic_error("slot not idle or READ still in flight");
    ++op.generation;
    op.request = make_request_id(cfg_.self, next_sequence_++);
    op.lock = pickers_.at(slot / cfg_.outstanding).next();
    op.begun = Clock::now();
    op.ordered = false; op.started = false;
    op.predecessor = 0; op.successor = 0;
    op.position = 0; op.successor_position = 0; op.log_slot = 0;
    requests_.emplace(op.request, slot);
    if (cfg_.backend == Backend::Cas) {
        post_cas(slot);
    } else if (cfg_.backend == Backend::Mu) {
        op.phase = Phase::WaitGrant;
        net_.send(cfg_.replicas.front(),
                  message(Kind::Acquire, cfg_.self, op.lock, op.request),
                  0, true, lock_lane(op.lock, cfg_.mu_lanes));
    } else {
        op.phase = Phase::WaitOrder;
        net_.send(cfg_.replicas.front(),
                  message(Kind::Acquire, cfg_.self, op.lock, op.request),
                  0, true, lock_lane(op.lock, cfg_.mu_lanes));
    }
}
void ClientEngine::begin_critical_section(size_t slot) {
    auto& op = ops_[slot];
    op.acquired = Clock::now();
    if (cfg_.critical_section_ops == 0) {
        op.phase = Phase::EmptyCriticalSection;
        empty_sections_.push_back(slot);
        return;
    }
    op.phase = Phase::Writing;
    *op.data_value = op.request;
    net_.write(cfg_.replicas.front(), op.data_value, sizeof(uint64_t),
               rdma::kDataOffset + op.lock * sizeof(uint64_t),
               track(slot, Purpose::DataWrite));
}
void ClientEngine::finish_empty_sections() {
    // Run before handling the next network event or submitting more work.
    // A local HANDOFF can append another slot, but cannot recursively release
    // it or reuse a slot in this drain. Every operation activates at most once.
    for (size_t i = 0; i < empty_sections_.size(); ++i) {
        if (empty_sections_.size() > ops_.size())
            throw std::runtime_error("empty critical section queue overflow");
        const size_t slot = empty_sections_[i];
        if (ops_.at(slot).phase != Phase::EmptyCriticalSection)
            throw std::runtime_error("invalid empty critical section state");
        release(slot);
    }
    empty_sections_.clear();
}
void ClientEngine::release(size_t slot) {
    auto& op = ops_[slot];
    if (op.phase != Phase::Writing && op.phase != Phase::EmptyCriticalSection)
        throw std::runtime_error("release before critical section completion");
    op.phase = Phase::WaitRelease;
    if (cfg_.backend == Backend::Handoff) {
        // The protected WRITE has completed, or no data work was issued.
        // Publish before
        // either the leader marker or HANDOFF, even if marker posting stalls.
        if (cfg_.handoff_peer_read) {
            auto* word = static_cast<uint64_t*>(net_.at(
                peer_completion_offset(op.request), sizeof(uint64_t)));
            std::atomic_ref<uint64_t>(*word).store(op.request, std::memory_order_release);
        }
        publish_retirement(slot);
    } else if (cfg_.backend == Backend::Mu) {
        net_.send(cfg_.replicas.front(),
                  message(Kind::Release, cfg_.self, op.lock, op.request),
                  0, true, lock_lane(op.lock, cfg_.mu_lanes));
    } else {
        NodeId owner = cfg_.replicas[op.lock % cfg_.replicas.size()];
        op.phase = Phase::WaitCasRelease;
        size_t offset = rdma::kFlagOffset + op.lock * sizeof(uint64_t);
        if (cfg_.cas_release_guarded) {
            net_.compare_swap(owner, op.cas_result, offset, 1, 0,
                              track(slot, Purpose::ReleaseCas));
        } else {
            net_.write(owner, op.release_zero, sizeof(uint64_t), offset,
                       track(slot, Purpose::ReleaseWrite));
        }
    }
}
void ClientEngine::complete(size_t slot) {
    auto& op = ops_[slot];
    auto now = Clock::now();
    if (op.begun >= measurement_begin_ && now <= measurement_end_) {
        ++completed_;
        acquire_ns_.push_back(nanos(op.acquired - op.begun));
        total_ns_.push_back(nanos(now - op.begun));
    }
    requests_.erase(op.request);
    op.phase = Phase::Idle;
}

void ClientEngine::deliver_handoff(RequestId predecessor, const Retired& record) {
    if (!record.successor) return;
    Message m = message(Kind::Handoff, cfg_.self, record.lock, record.successor);
    m.other = predecessor;
    m.position = record.successor_position;
    ++handoffs_sent_;
    const NodeId peer = request_node(record.successor);
    if (peer == cfg_.self) {
        rdma::Completion c{};
        c.kind = rdma::CompletionKind::Receive;
        c.peer = cfg_.self;
        c.message = m;
        handle_activation(c);
    } else {
        net_.send(peer, m);
    }
}
void ClientEngine::publish_retirement(size_t slot) {
    auto& op = ops_.at(slot);
    if (!op.ordered || op.phase != Phase::WaitRelease)
        throw std::runtime_error("release before ordered, quiesced ownership");
    // Inline data needs no registered buffer, but the WR must still fit
    // within the bounded marker budget. Preserve deferred-slot backpressure.
    if (markers_posted_ - markers_completed_ >= marker_credit_) {
        deferred_marker_slots_.push_back(slot);
        return;
    }
    if (markers_posted_ + 1 >= kMarkerTokenBase)
        throw std::runtime_error("marker work ID exhausted");
    Retired record{op.lock, op.position, op.successor, op.successor_position};
    auto [it, added] = retired_.emplace(op.request, record);
    if (!added) throw std::runtime_error("duplicate retirement");
    // Protected data work is quiescent before entering this function.
    if (it->second.successor)
        deliver_handoff(op.request, it->second);
    const size_t remote_offset = handoff_marker_offset(
        net_.remote_memory_bytes(cfg_.replicas.front()), op.log_slot);
    const WorkId work = kMarkerTokenBase + markers_posted_ + 1;
    if (!net_.write_inline(cfg_.replicas.front(), op.request, remote_offset, work))
        throw std::runtime_error("marker WRITE must be signaled");
    ++markers_posted_;
    complete(slot); // Local release is not blocked on marker completion.
}
void ClientEngine::handle_order(const rdma::Completion& c) {
    const Message& m = c.message;
    auto it = requests_.find(m.request);
    if (it == requests_.end()) throw std::runtime_error("ORDER for unknown request");
    auto& op = ops_.at(it->second);
    if (op.phase != Phase::WaitOrder || op.ordered || op.lock != m.lock_id)
        throw std::runtime_error("invalid ORDER");
    op.ordered = true;
    op.position = m.position;
    op.log_slot = m.value;
    op.predecessor = m.other;
    if (auto found = early_next_.find(m.request); found != early_next_.end()) {
        Message earlier = found->second;
        early_next_.erase(found);
        rdma::Completion saved{};
        saved.kind = rdma::CompletionKind::Receive;
        saved.peer = cfg_.replicas.front(); saved.message = earlier;
        handle_next(saved);
    }
    if (auto found = early_start_.find(m.request); found != early_start_.end()) {
        Message earlier = found->second;
        early_start_.erase(found);
        rdma::Completion saved{};
        saved.kind = rdma::CompletionKind::Receive;
        saved.peer = cfg_.replicas.front(); saved.message = earlier;
        handle_activation(saved);
    } else if (op.predecessor == 0) {
        // A committed predecessor-free ORDER acts as the start signal.
        rdma::Completion synthetic{};
        synthetic.kind = rdma::CompletionKind::Receive;
        synthetic.peer = cfg_.replicas.front();
        synthetic.message = message(Kind::Start, cfg_.replicas.front(),
                                    op.lock, op.request);
        synthetic.message.position = op.position;
        handle_activation(synthetic);
    }
    if (auto found = early_handoff_.find(m.request); found != early_handoff_.end()) {
        Message earlier = found->second;
        early_handoff_.erase(found);
        rdma::Completion saved{};
        saved.kind = rdma::CompletionKind::Receive;
        saved.peer = earlier.sender; saved.message = earlier;
        handle_activation(saved);
    }
    if (cfg_.handoff_peer_read && !op.started && op.predecessor)
        try_predecessor_read(it->second);
}
void ClientEngine::activate(size_t slot) {
    auto& op = ops_.at(slot);
    if (!op.ordered || op.started || op.phase != Phase::WaitOrder)
        throw std::runtime_error("invalid activation state");
    op.started = true;
    begin_critical_section(slot);
}
void ClientEngine::try_predecessor_read(size_t slot) {
    auto& op = ops_.at(slot);
    if (!op.ordered || op.started || !op.predecessor || op.read_pending)
        throw std::runtime_error("invalid predecessor READ state");
    const NodeId peer = request_node(op.predecessor);
    if (peer == 0 || peer >= peer_reads_.size() ||
        std::find(cfg_.replicas.begin(), cfg_.replicas.end(), peer) != cfg_.replicas.end())
        throw std::runtime_error("invalid predecessor client");
    // Bound both pending READs and the receipt ledger. One remote READ per
    // peer at a time (the transport negotiates max_rd_atomic=1). No polling
    // loop and no queue of READ retries; HANDOFF remains the fallback.
    if (read_receipts_.size() + reads_inflight_ >= PeerReadReceipts::limit ||
        (peer != cfg_.self && peer_reads_[peer] != 0)) {
        ++read_skipped_;
        return;
    }
    const size_t offset = peer_completion_offset(op.predecessor);
    if (peer == cfg_.self) {
        ++local_probes_;
        auto* word = static_cast<uint64_t*>(net_.at(offset, sizeof(uint64_t)));
        observe_predecessor(slot, std::atomic_ref<uint64_t>(*word).load(
            std::memory_order_acquire));
        return;
    }
    ++read_attempts_;
    ++peer_reads_[peer];
    ++reads_inflight_;
    op.read_pending = true;
    net_.read_word(peer, op.read_result, offset, track(slot, Purpose::PredecessorRead));
}
void ClientEngine::observe_predecessor(size_t slot, uint64_t value) {
    auto& op = ops_.at(slot);
    if (!peer_completion_matches(op.predecessor, value)) {
        ++read_misses_;
        return;
    }
    if (op.started) return; // HANDOFF already won; no second protected WRITE.
    read_receipts_.expect(op.request, op.lock, op.predecessor, op.position);
    ++activation_by_read_;
    activate(slot);
}
void ClientEngine::handle_activation(const rdma::Completion& c) {
    const Message& m = c.message;
    // Match against the exact committed proof even after B's slot is reused.
    if (cfg_.handoff_peer_read && message_kind(m) == Kind::Handoff &&
        read_receipts_.consume(c.peer, m)) {
        ++handoffs_received_;
        ++redundant_handoffs_;
        return;
    }
    auto it = requests_.find(m.request);
    if (it == requests_.end()) throw std::runtime_error("activation for unknown request");
    auto& op = ops_.at(it->second);
    if (op.lock != m.lock_id)
        throw std::runtime_error("activation lock mismatch");
    const Kind kind = message_kind(m);
    if (kind != Kind::Start && kind != Kind::Handoff)
        throw std::runtime_error("invalid activation kind");
    if (!op.ordered) {
        auto& pending = kind == Kind::Start ? early_start_ : early_handoff_;
        if (!pending.emplace(m.request, m).second)
            throw std::runtime_error("duplicate early activation");
        return;
    }
    if (op.phase != Phase::WaitOrder || op.started || m.position != op.position)
        throw std::runtime_error("duplicate or mismatched activation");
    if (kind == Kind::Start) {
        if (c.peer != cfg_.replicas.front() || op.predecessor != 0)
            throw std::runtime_error("unauthorized START");
    } else {
        if (!op.predecessor || m.other != op.predecessor ||
            c.peer != request_node(op.predecessor))
            throw std::runtime_error("unauthorized HANDOFF");
        ++handoffs_received_;
        ++activation_by_handoff_;
    }
    activate(it->second);
}
void ClientEngine::handle_next(const rdma::Completion& c) {
    const Message& m = c.message;
    if (c.peer != cfg_.replicas.front() || message_kind(m) != Kind::Next ||
        !m.other || m.request == m.other || m.value != m.position + 1)
        throw std::runtime_error("invalid NEXT");
    if (auto it = requests_.find(m.request); it != requests_.end()) {
        auto& op = ops_.at(it->second);
        if (op.lock != m.lock_id) throw std::runtime_error("NEXT lock mismatch");
        if (!op.ordered) {
            if (!early_next_.emplace(m.request, m).second)
                throw std::runtime_error("duplicate early NEXT");
            return;
        }
        if (op.position != m.position || op.successor)
            throw std::runtime_error("mismatched or duplicate NEXT");
        op.successor = m.other;
        op.successor_position = m.value;
        return;
    }
    auto it = retired_.find(m.request);
    if (it == retired_.end())
        throw std::runtime_error("NEXT after acknowledged retirement");
    if (it->second.lock != m.lock_id ||
        it->second.position != m.position || it->second.successor)
        throw std::runtime_error("invalid late NEXT");
    it->second.successor = m.other;
    it->second.successor_position = m.value;
    ++late_next_;
    deliver_handoff(m.request, it->second);
}

void ClientEngine::handle(const rdma::Completion& c) {
    if (c.kind == rdma::CompletionKind::Send) return;
    if (c.kind == rdma::CompletionKind::Receive) {
        if (c.message.sender != c.peer)
            throw std::runtime_error("message sender mismatch");
        const Kind kind = message_kind(c.message);
        if (c.peer == cfg_.replicas.front()) {
            const bool control = kind == Kind::Ready || kind == Kind::Stop;
            if (control) {
                if (c.lane != 0)
                    throw std::runtime_error("barrier reply arrived on data lane");
            } else if (cfg_.backend != Backend::Cas &&
                       c.lane != lock_lane(c.message.lock_id, cfg_.mu_lanes)) {
                throw std::runtime_error("leader reply arrived on wrong client lane");
            }
        }
        if (kind == Kind::Ready) return;
        if (kind == Kind::Stop) {
            if (c.peer != cfg_.replicas.front() || !stop_sent_ || server_stopped_)
                throw std::runtime_error("invalid shutdown barrier");
            server_stopped_ = true;
            return;
        }
        if (cfg_.backend == Backend::Handoff) {
            if (kind == Kind::Handoff) {
                handle_activation(c);
                return;
            }
            if (c.peer != cfg_.replicas.front())
                throw std::runtime_error("handoff control from non-leader");
            if (kind == Kind::Order) { handle_order(c); return; }
            if (kind == Kind::Start) { handle_activation(c); return; }
            if (kind == Kind::Next) { handle_next(c); return; }
            if (kind == Kind::ReleaseAck) {
                auto it = retired_.find(c.message.request);
                if (it == retired_.end() || it->second.lock != c.message.lock_id ||
                    it->second.position != c.message.position)
                    throw std::runtime_error("invalid retirement ACK");
                // NEXT (if any) precedes this ACK on the same ordered client
                // QP; the retired predecessor is no longer in the leader queue.
                retired_.erase(it);
                return;
            }
            throw std::runtime_error("unexpected handoff control message");
        }
        if (c.peer != cfg_.replicas.front())
            throw std::runtime_error("unexpected server message");
        auto it = requests_.find(c.message.request);
        if (it == requests_.end()) throw std::runtime_error("unknown request response");
        auto slot = it->second;
        auto& op = ops_[slot];
        if (op.lock != c.message.lock_id)
            throw std::runtime_error("lock ID mismatch");
        if (kind == Kind::Grant && op.phase == Phase::WaitGrant)
            begin_critical_section(slot);
        else if (kind == Kind::ReleaseAck && op.phase == Phase::WaitRelease)
            complete(slot);
        else
            throw std::runtime_error("unexpected response or phase");
        return;
    }
    if (c.work_id >= kMarkerTokenBase) {
        if (cfg_.backend != Backend::Handoff ||
            c.kind != rdma::CompletionKind::Write || c.peer != cfg_.replicas.front() ||
            markers_drained() || c.work_id != kMarkerTokenBase + markers_completed_ + 1)
            throw std::runtime_error("invalid inline marker completion");
        ++markers_completed_;
        if (!deferred_marker_slots_.empty()) {
            const size_t slot = deferred_marker_slots_.front();
            deferred_marker_slots_.pop_front();
            publish_retirement(slot);
        }
        return;
    }
    auto it = awaited_.find(c.work_id);
    if (it == awaited_.end()) throw std::runtime_error("unknown local completion");
    Awaited wait = it->second;
    awaited_.erase(it);
    auto& op = ops_.at(wait.slot);
    if (op.generation != wait.generation)
        throw std::runtime_error("stale completion generation");
    switch (wait.purpose) {
    case Purpose::PredecessorRead: {
        const NodeId peer = request_node(op.predecessor);
        if (c.kind != rdma::CompletionKind::Read || c.peer != peer ||
            peer >= peer_reads_.size() || !op.read_pending ||
            !peer_reads_[peer] || !reads_inflight_)
            throw std::runtime_error("invalid predecessor READ completion");
        op.read_pending = false;
        --peer_reads_[peer];
        --reads_inflight_;
        // HANDOFF may have won and the operation may have completed while
        // the DMA result was outstanding. The slot cannot yet be reused.
        observe_predecessor(wait.slot, *op.read_result);
        break;
    }
    case Purpose::AcquireCas:
        if (c.kind != rdma::CompletionKind::CompareSwap ||
            op.phase != Phase::WaitCas)
            throw std::runtime_error("invalid acquire CAS completion");
        if (*op.cas_result == 0) begin_critical_section(wait.slot);
        else if (*op.cas_result == 1) post_cas(wait.slot);
        else throw std::runtime_error("CAS returned invalid lock value");
        break;
    case Purpose::DataWrite:
        if (c.kind != rdma::CompletionKind::Write || op.phase != Phase::Writing)
            throw std::runtime_error("invalid data WRITE completion");
        release(wait.slot);
        break;
    case Purpose::ReleaseCas:
        if (c.kind != rdma::CompletionKind::CompareSwap ||
            op.phase != Phase::WaitCasRelease || *op.cas_result != 1)
            throw std::runtime_error("invalid release CAS completion");
        complete(wait.slot);
        break;
    case Purpose::ReleaseWrite:
        if (c.kind != rdma::CompletionKind::Write || op.phase != Phase::WaitCasRelease)
            throw std::runtime_error("invalid release WRITE completion");
        complete(wait.slot);
        break;
    }
}
void ClientEngine::run() {
    const NodeId leader = cfg_.replicas.front();
    net_.send(leader, message(Kind::Ready, cfg_.self));
    auto barrier_deadline = Clock::now() + std::chrono::seconds(cfg_.watchdog_seconds);
    bool barrier = false;
    std::vector<rdma::Completion> completions;
    completions.reserve(64);
    while (!barrier && Clock::now() < barrier_deadline) {
        net_.poll_into(completions);
        for (const auto& e : completions) {
            if (e.kind == rdma::CompletionKind::Receive) {
                if (e.peer != leader || e.message.sender != leader ||
                    message_kind(e.message) != Kind::Ready)
                    throw std::runtime_error("invalid start barrier");
                barrier = true;
            }
        }
    }
    if (!barrier) throw std::runtime_error("client start barrier timeout");
    const auto start = Clock::now();
    measurement_begin_ = start + std::chrono::seconds(cfg_.warmup_seconds);
    measurement_end_ = measurement_begin_ + std::chrono::seconds(cfg_.duration_seconds);
    auto watchdog = measurement_end_ + std::chrono::seconds(cfg_.watchdog_seconds);
    while (true) {
        auto now = Clock::now();
        if (now > watchdog) throw std::runtime_error("client watchdog timeout: fail closed");
        if (now < measurement_end_) {
            for (size_t i = 0; i < ops_.size(); ++i) {
                if (ops_[i].phase == Phase::Idle && !ops_[i].read_pending) submit(i);
            }
        }
        net_.poll_into(completions);
        for (auto& event : completions) {
            handle(event);
            if (!empty_sections_.empty()) finish_empty_sections();
        }
        if (now >= measurement_end_ && requests_.empty() &&
            markers_drained() && deferred_marker_slots_.empty() &&
            retired_.empty() && awaited_.empty() && read_receipts_.empty() &&
            empty_sections_.empty() &&
            !stop_sent_) {
            // Every read winner has consumed its normal HANDOFF, all local
            // DMA has completed, and all retirements have been acknowledged.
            // Keep the registered region alive until the all-client barrier.
            net_.send(leader, message(Kind::Stop, cfg_.self));
            stop_sent_ = true;
        }
        if (stop_sent_ && server_stopped_) break;
    }
    if (!awaited_.empty()) throw std::runtime_error("outstanding CQEs at completion");
    print_result();
}
void ClientEngine::print_result() const {
    const char* name = cfg_.backend == Backend::Cas ? "cas" :
                       cfg_.backend == Backend::Mu ? "mu" : "handoff";
    std::cout << "RESULT {"
              << "\"node\":" << cfg_.self
              << ",\"backend\":\"" << name << "\""
              << ",\"locks\":" << cfg_.lock_count
              << ",\"critical_section_ops\":" << cfg_.critical_section_ops
              << ",\"workers\":" << cfg_.workers
              << ",\"outstanding_per_worker\":" << cfg_.outstanding
              << ",\"completed\":" << completed_
              << ",\"measurement_seconds\":" << cfg_.duration_seconds
              << ",\"acquire_p50_ns\":" << percentile(acquire_ns_, 0.50)
              << ",\"acquire_p99_ns\":" << percentile(acquire_ns_, 0.99)
              << ",\"acquire_p999_ns\":" << percentile(acquire_ns_, 0.999)
              << ",\"total_p50_ns\":" << percentile(total_ns_, 0.50)
              << ",\"total_p99_ns\":" << percentile(total_ns_, 0.99)
              << ",\"total_p999_ns\":" << percentile(total_ns_, 0.999)
              << ",\"handoffs_sent\":" << handoffs_sent_
              << ",\"handoffs_received\":" << handoffs_received_
              << ",\"late_next\":" << late_next_
              << ",\"activation_by_read\":" << activation_by_read_
              << ",\"activation_by_handoff\":" << activation_by_handoff_
              << ",\"read_attempts\":" << read_attempts_
              << ",\"local_probes\":" << local_probes_
              << ",\"read_misses\":" << read_misses_
              << ",\"read_skipped\":" << read_skipped_
              << ",\"redundant_handoffs\":" << redundant_handoffs_
              << "}" << std::endl;
}
} // namespace lock
