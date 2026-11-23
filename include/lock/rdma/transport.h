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
enum class CompletionKind { Receive, Send, Write, CompareSwap };
struct Completion {
    CompletionKind kind;
    NodeId peer;
    WorkId work_id = 0;
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
    void send(NodeId peer, const Message& m, WorkId work_id = 0);
    void write(NodeId peer, const void* local, size_t length,
               size_t remote_offset, WorkId work_id);
    void compare_swap(NodeId peer, uint64_t* result, size_t remote_offset,
                      uint64_t expected, uint64_t desired, WorkId work_id);
    std::vector<Completion> poll(int batch = 64);

    // Offset-addressed memory is registered for LOCAL_WRITE and remote access.
    void* at(size_t offset, size_t length);
    const void* at(size_t offset, size_t length) const;
    void* allocate(size_t length, size_t alignment = 64);
    size_t memory_bytes() const;
    NodeId self() const;
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
} // namespace lock::rdma
