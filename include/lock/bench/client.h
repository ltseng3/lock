#pragma once
#include "lock/bench/workload.h"
#include "lock/locks/backend.h"
#include "lock/rdma/transport.h"
#include <chrono>
#include <cstdint>
#include <deque>
#include <unordered_map>
#include <vector>
namespace lock {
struct ClientConfig {
    Backend backend = Backend::Cas;
    NodeId self = 0;
    std::vector<NodeId> replicas;
    size_t lock_count = 1000;
    size_t outstanding = 1;
    size_t workers = 1;
    size_t mu_lanes = 1;
    std::string distribution = "uniform";
    uint64_t seed = 2026;
    uint64_t warmup_seconds = 5;
    uint64_t duration_seconds = 15;
    uint64_t watchdog_seconds = 90;
    bool cas_release_guarded = false;
};
class ClientEngine {
public:
    ClientEngine(rdma::Transport& net, ClientConfig cfg);
    void run();
private:
    using Clock = std::chrono::steady_clock;
    enum class Phase { Idle, WaitGrant, WaitOrder, WaitCas, Writing, WaitRelease, WaitCasRelease };
    enum class Purpose { AcquireCas, DataWrite, ReleaseCas, ReleaseWrite };
    struct Operation {
        Phase phase = Phase::Idle;
        RequestId request = 0;
        LockId lock = 0;
        uint64_t generation = 0;
        bool ordered = false;
        bool started = false;
        RequestId predecessor = 0;
        RequestId successor = 0;
        uint64_t position = 0;
        uint64_t successor_position = 0;
        uint64_t log_slot = 0;
        uint64_t* cas_result = nullptr;
        uint64_t* data_value = nullptr;
        uint64_t* release_zero = nullptr;
        Clock::time_point begun{};
        Clock::time_point acquired{};
    };
    struct Awaited {
        size_t slot;
        uint64_t generation;
        Purpose purpose;
    };
    struct Retired {
        LockId lock = 0;
        uint64_t position = 0;
        RequestId successor = 0;
        uint64_t successor_position = 0;
        bool sent = false;
        bool acknowledged = false;
    };
    struct Marker {
        uint64_t* payload = nullptr;
        bool occupied = false;
    };
    rdma::Transport& net_;
    ClientConfig cfg_;
    std::vector<LockPicker> pickers_;
    std::vector<Operation> ops_;
    std::unordered_map<RequestId, size_t> requests_;
    std::unordered_map<WorkId, Awaited> awaited_;
    std::unordered_map<RequestId, Retired> retired_;
    std::unordered_map<RequestId, Message> early_start_;
    std::unordered_map<RequestId, Message> early_handoff_;
    std::unordered_map<RequestId, Message> early_next_;
    std::vector<Marker> markers_;
    std::vector<size_t> free_markers_;
    // A marker WRITE owns its buffer until its local CQE. If the pool is
    // full, retain the operation slot and post its marker on the next CQE.
    std::deque<size_t> deferred_marker_slots_;
    std::unordered_map<WorkId, size_t> marker_work_;
    uint64_t handoffs_sent_ = 0;
    uint64_t handoffs_received_ = 0;
    uint64_t late_next_ = 0;
    bool stop_sent_ = false;
    bool server_stopped_ = false;
    uint64_t next_sequence_ = 1;
    uint64_t next_work_ = 1;
    Clock::time_point measurement_begin_{}, measurement_end_{};
    size_t completed_ = 0;
    std::vector<uint64_t> acquire_ns_, total_ns_;
    WorkId track(size_t slot, Purpose purpose);
    void submit(size_t slot);
    void post_cas(size_t slot);
    void post_data(size_t slot);
    void release(size_t slot);
    void complete(size_t slot);
    void handle_order(const rdma::Completion& event);
    void handle_activation(const rdma::Completion& event);
    void handle_next(const rdma::Completion& event);
    void deliver_handoff(RequestId predecessor, const Retired& record);
    void publish_retirement(size_t slot);
    void handle(const rdma::Completion& c);
    void print_result() const;
};
} // namespace lock
