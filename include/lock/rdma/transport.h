#pragma once
#include "lock/types.h"
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace lock::rdma {
constexpr size_t kFlagOffset = 0x10000;
constexpr size_t kDataOffset = 0x20000;
constexpr size_t kLogOffset = 0x100000;
constexpr size_t kScratchOffset = 64ULL * 1024 * 1024;
constexpr size_t kDefaultMemoryBytes = 128ULL * 1024 * 1024;

struct Peer {
    NodeId id;
    std::string ip;
};
enum class CompletionKind { Receive, Send, Write, CompareSwap, Read };
struct Completion {
    CompletionKind kind;
    NodeId peer;
    WorkId work_id = 0;
    size_t lane = 0; // receive QP lane; zero for the standard channel
    Message message{};
};
struct Config {
    NodeId self = 0;
    std::vector<Peer> peers;
    uint16_t tcp_port = 18515;
    uint8_t ib_port = 1;
    std::string device;
    size_t memory_bytes = kDefaultMemoryBytes;
    uint32_t receive_depth = 128;
    // Leader-only Mu configuration: these peer QPs get a dedicated
    // replication CQ, separate from the client-communication CQ.
    std::vector<NodeId> replication_peers;
    // Replica-to-replica links have this many separate QPs/CQs. Non-replica
    // peers retain one QP and the standard bootstrap protocol.
    size_t replication_lanes = 1;
    // Only leader<->client peers use the additional bidirectional lane QPs.
    // On the leader each lane has its own CQ; clients can poll a shared CQ.
    // Client-to-client HANDOFF and replica data WRITEs retain lane zero.
    std::vector<NodeId> client_lane_peers;
};

std::vector<Peer> parse_peers(const std::string& spec);

class Transport {
public:
    explicit Transport(Config config);
    ~Transport();
    Transport(const Transport&) = delete;
    Transport& operator=(const Transport&) = delete;

    // All peers must enter connect() during startup. No connection in the measured path.
    void connect();
    // Unsignaled SEND/WRITE requests are periodically signaled to retire QP WRs.
    // WRITE returns whether a CQE was requested; the caller must only track those.
    void send(NodeId peer, const Message& m, WorkId work_id = 0,
              bool signal = true, size_t lane = 0);
    bool write(NodeId peer, const void* local, size_t length,
               size_t remote_offset, WorkId work_id, bool signal = true);
    // The eight-byte source is copied inline by the NIC at posting time;
    // it does not need a registered backing allocation.
    bool write_inline(NodeId peer, uint64_t value, size_t remote_offset,
                      WorkId work_id, bool signal = true,
                      size_t lane = 0);
    // A signaled, eight-byte peer READ into registered, aligned storage.
    void read_word(NodeId peer, uint64_t* result, size_t remote_offset,
                   WorkId work_id);
    void compare_swap(NodeId peer, uint64_t* result, size_t remote_offset,
                      uint64_t expected, uint64_t desired, WorkId work_id);
    // Reuse the caller's completion storage on the hot path.
    void poll_into(std::vector<Completion>& out, int batch = 64);
    void poll_replication_into(std::vector<Completion>& out, int batch = 64,
                               size_t lane = 0);
    // Each leader communication worker exclusively polls its own lane CQ.
    void poll_communication_into(std::vector<Completion>& out, int batch = 64,
                                 size_t lane = 0);
    std::vector<Completion> poll(int batch = 64);

    // Offset-addressed memory is registered for LOCAL_WRITE and remote access.
    void* at(size_t offset, size_t length);
    const void* at(size_t offset, size_t length) const;
    void* allocate(size_t length, size_t alignment = 64);
    size_t memory_bytes() const;
    size_t remote_memory_bytes(NodeId peer) const;
    NodeId self() const;
private:
    enum class PollGroup { Default, Replication, Communication };
    void poll_on_cq(std::vector<Completion>& out, int batch,
                    PollGroup group, size_t lane);
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
} // namespace lock::rdma
