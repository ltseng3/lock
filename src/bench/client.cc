#include "lock/bench/client.h"
#include <algorithm>
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
      picker_(cfg_.lock_count, cfg_.distribution,
              cfg_.seed ^ (static_cast<uint64_t>(cfg_.self) << 32)),
      ops_(cfg_.outstanding) {
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
    op.lock = picker_.next();
    op.begun = Clock::now();
    requests_.emplace(op.request, slot);
    if (cfg_.backend == Backend::Cas) {
        post_cas(slot);
    } else if (cfg_.backend == Backend::Mu) {
        op.phase = Phase::WaitGrant;
        net_.send(cfg_.replicas.front(),
                  message(Kind::Acquire, cfg_.self, op.lock, op.request));
    } else {
        throw std::runtime_error("handoff backend not enabled in this revision");
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
    if (cfg_.backend == Backend::Mu) {
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
void ClientEngine::handle(const rdma::Completion& c) {
    if (c.kind == rdma::CompletionKind::Send) return;
    if (c.kind == rdma::CompletionKind::Receive) {
        if (c.message.sender != c.peer)
            throw std::runtime_error("message sender mismatch");
        const Kind kind = message_kind(c.message);
        if (kind == Kind::Ready) return;
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
    while (!barrier && Clock::now() < barrier_deadline) {
        for (const auto& e : net_.poll()) {
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
        for (auto& event : net_.poll()) handle(event);
        if (now >= measurement_end_ && requests_.empty()) break;
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
              << ",\"completed\":" << completed_
              << ",\"measurement_seconds\":" << cfg_.duration_seconds
              << ",\"acquire_p50_ns\":" << percentile(acquire_ns_, 0.50)
              << ",\"acquire_p99_ns\":" << percentile(acquire_ns_, 0.99)
              << ",\"acquire_p999_ns\":" << percentile(acquire_ns_, 0.999)
              << ",\"total_p50_ns\":" << percentile(total_ns_, 0.50)
              << ",\"total_p99_ns\":" << percentile(total_ns_, 0.99)
              << ",\"total_p999_ns\":" << percentile(total_ns_, 0.999)
              << "}" << std::endl;
}
} // namespace lock
