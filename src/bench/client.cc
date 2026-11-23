#include "lock/bench/client.h"
#include <algorithm>
#include "lock/locks/replicated_log.h"
#include "lock/locks/handoff_layout.h"
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
    pickers_.reserve(cfg_.workers);
    for (size_t worker = 0; worker < cfg_.workers; ++worker) {
        pickers_.emplace_back(cfg_.lock_count, cfg_.distribution,
            cfg_.seed ^ (static_cast<uint64_t>(cfg_.self) << 32) ^
            (worker * 0x9e3779b97f4a7c15ULL));
    }
    if (cfg_.replicas.size() != 3 && cfg_.replicas.size() != 5)
        throw std::invalid_argument("three or five replicas required");
    if (cfg_.outstanding == 0 || cfg_.outstanding > 512 || cfg_.lock_count == 0 ||
        cfg_.lock_count > 1000)
        throw std::invalid_argument("invalid client window or lock count");
    for (auto& op : ops_) {
        op.cas_result = static_cast<uint64_t*>(net_.allocate(sizeof(uint64_t), 8));
        op.data_value = static_cast<uint64_t*>(net_.allocate(sizeof(uint64_t), 8));
        op.release_zero = static_cast<uint64_t*>(net_.allocate(sizeof(uint64_t), 8));
        *op.release_zero = 0;
    }
    if (cfg_.backend == Backend::Handoff) {
        const size_t count = std::max<size_t>(128, cfg_.outstanding * 4);
        markers_.resize(count);
        for (auto& marker : markers_)
            marker.payload = static_cast<uint64_t*>(net_.allocate(sizeof(uint64_t), 8));
    }
}
WorkId ClientEngine::track(size_t slot, Purpose purpose) {
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
    if (op.phase != Phase::Idle) throw std::logic_error("slot not idle");
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
                  message(Kind::Acquire, cfg_.self, op.lock, op.request));
    } else {
        op.phase = Phase::WaitOrder;
        net_.send(cfg_.replicas.front(),
                  message(Kind::Acquire, cfg_.self, op.lock, op.request));
    }
}
void ClientEngine::post_data(size_t slot) {
    auto& op = ops_[slot];
    op.acquired = Clock::now();
    op.phase = Phase::Writing;
    *op.data_value = op.request;
    net_.write(cfg_.replicas.front(), op.data_value, sizeof(uint64_t),
               rdma::kDataOffset + op.lock * sizeof(uint64_t),
               track(slot, Purpose::DataWrite));
}
void ClientEngine::release(size_t slot) {
    auto& op = ops_[slot];
    op.phase = Phase::WaitRelease;
    if (cfg_.backend == Backend::Handoff) {
        publish_retirement(slot);
    } else if (cfg_.backend == Backend::Mu) {
        net_.send(cfg_.replicas.front(),
                  message(Kind::Release, cfg_.self, op.lock, op.request));
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
    Retired record{op.lock, op.position, op.successor, op.successor_position, false};
    auto [it, added] = retired_.emplace(op.request, record);
    if (!added) throw std::runtime_error("duplicate retirement");
    // Protected data work was completed before entering this function.
    if (it->second.successor) {
        deliver_handoff(op.request, it->second);
        it->second.sent = true;
    }
    size_t i = 0;
    while (i < markers_.size() && markers_[i].occupied) ++i;
    if (i == markers_.size())
        throw std::runtime_error("marker-buffer pool exhausted: fail closed");
    markers_[i].occupied = true;
    *markers_[i].payload = op.request;
    WorkId work = next_work_++;
    marker_work_.emplace(work, i);
    const size_t remote_offset = handoff_marker_offset(
        net_.remote_memory_bytes(cfg_.replicas.front()), op.log_slot);
    net_.write(cfg_.replicas.front(), markers_[i].payload, sizeof(uint64_t),
               remote_offset, work);
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
}
void ClientEngine::handle_activation(const rdma::Completion& c) {
    const Message& m = c.message;
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
    }
    op.started = true;
    post_data(it->second);
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
    it->second.sent = true;
    if (it->second.acknowledged) retired_.erase(it);
}

void ClientEngine::handle(const rdma::Completion& c) {
    if (c.kind == rdma::CompletionKind::Send) return;
    if (c.kind == rdma::CompletionKind::Receive) {
        if (c.message.sender != c.peer)
            throw std::runtime_error("message sender mismatch");
        const Kind kind = message_kind(c.message);
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
                if (it->second.acknowledged)
                    throw std::runtime_error("duplicate retirement ACK");
                it->second.acknowledged = true;
                // If no successor exists yet, keep the retired predecessor
                // alive for an ORDER/NEXT committed after its release.
                if (it->second.sent) retired_.erase(it);
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
            post_data(slot);
        else if (kind == Kind::ReleaseAck && op.phase == Phase::WaitRelease)
            complete(slot);
        else
            throw std::runtime_error("unexpected response or phase");
        return;
    }
    if (auto marker = marker_work_.find(c.work_id); marker != marker_work_.end()) {
        if (c.kind != rdma::CompletionKind::Write)
            throw std::runtime_error("release marker completion not WRITE");
        markers_[marker->second].occupied = false;
        marker_work_.erase(marker);
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
    case Purpose::AcquireCas:
        if (c.kind != rdma::CompletionKind::CompareSwap ||
            op.phase != Phase::WaitCas)
            throw std::runtime_error("invalid acquire CAS completion");
        if (*op.cas_result == 0) post_data(wait.slot);
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
                if (ops_[i].phase == Phase::Idle) submit(i);
            }
        }
        net_.poll_into(completions);
        for (auto& event : completions) handle(event);
        if (now >= measurement_end_ && requests_.empty() &&
            marker_work_.empty() && !stop_sent_) {
            bool all_acknowledged = true;
            for (const auto& [id, retired] : retired_) {
                if (!retired.acknowledged) {
                    all_acknowledged = false;
                    break;
                }
            }
            if (all_acknowledged) {
                // A client is done issuing work but must keep serving late
                // NEXT until every client has finished and the leader closes
                // the shared queue. All marker writes have completed locally.
                net_.send(leader, message(Kind::Stop, cfg_.self));
                stop_sent_ = true;
            }
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
              << "}" << std::endl;
}
} // namespace lock
