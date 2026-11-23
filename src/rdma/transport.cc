#include "lock/rdma/transport.h"
#include <infiniband/verbs.h>
#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <map>
#include <netinet/in.h>
#include <stdexcept>
#include <string>
#include <sys/socket.h>
#include <sys/resource.h>
#include <thread>
#include <unistd.h>
#include <vector>
#include <algorithm>
#include <array>

namespace lock::rdma {
namespace {
constexpr uint32_t kMagic = 0x52444d41;
constexpr uint64_t kTokenMask = (1ULL << 48) - 1;
constexpr uint32_t kMaxSend = 1024;
struct Bootstrap {
    uint32_t magic, version, node, qpn;
    uint32_t psn, rkey, lid, reserved;
    uint64_t base, bytes;
};
static_assert(sizeof(Bootstrap) == 48);
enum class Tag : uint64_t { Receive = 1, Send = 2, Write = 3, Cas = 4 };
uint64_t encode(Tag tag, NodeId peer, uint64_t token) {
    if (peer > 255 || token > kTokenMask)
        throw std::runtime_error("RDMA work ID exceeds encoding range");
    return (static_cast<uint64_t>(tag) << 56) |
           (static_cast<uint64_t>(peer) << 48) | token;
}
uint64_t align_up(uint64_t x, size_t alignment) {
    if (!alignment || (alignment & (alignment - 1)))
        throw std::invalid_argument("alignment must be a power of two");
    return (x + alignment - 1) & ~(static_cast<uint64_t>(alignment) - 1);
}
void check(bool ok, const char* action) {
    if (!ok) throw std::runtime_error(std::string("RDMA: ") + action +
                                     ": errno=" + std::to_string(errno));
}
void send_all(int fd, const void* ptr, size_t bytes) {
    auto* p = static_cast<const char*>(ptr);
    while (bytes) {
        ssize_t n = ::send(fd, p, bytes, MSG_NOSIGNAL);
        check(n > 0, "TCP send");
        p += n;
        bytes -= static_cast<size_t>(n);
    }
}
void read_all(int fd, void* ptr, size_t bytes) {
    auto* p = static_cast<char*>(ptr);
    while (bytes) {
        ssize_t n = ::recv(fd, p, bytes, 0);
        check(n > 0, "TCP receive");
        p += n;
        bytes -= static_cast<size_t>(n);
    }
}
int listener(uint16_t port) {
    int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    check(fd >= 0, "socket");
    int one = 1;
    ::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = INADDR_ANY;
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 ||
        ::listen(fd, 64) != 0) {
        ::close(fd);
        check(false, "TCP listen");
    }
    return fd;
}
int dial(const std::string& ip, uint16_t port) {
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    check(::inet_pton(AF_INET, ip.c_str(), &addr.sin_addr) == 1, "invalid IPv4 address");
    for (int attempt = 0; attempt < 600; ++attempt) {
        int fd = ::socket(AF_INET, SOCK_STREAM, 0);
        check(fd >= 0, "socket");
        if (::connect(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) == 0)
            return fd;
        ::close(fd);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    throw std::runtime_error("TCP connect failed to " + ip);
}
} // namespace

std::vector<Peer> parse_peers(const std::string& spec) {
    std::vector<Peer> result;
    size_t pos = 0;
    while (pos < spec.size()) {
        size_t end = spec.find(',', pos);
        std::string token = spec.substr(pos, end == std::string::npos ? end : end - pos);
        size_t at = token.find('@');
        if (at == std::string::npos || at == 0 || at + 1 == token.size())
            throw std::invalid_argument("peers must be id@IPv4,id@IPv4");
        size_t parsed = 0;
        auto id = std::stoul(token.substr(0, at), &parsed);
        if (parsed != at || id == 0 || id > 255)
            throw std::invalid_argument("peer id must be 1..255");
        result.push_back({static_cast<NodeId>(id), token.substr(at + 1)});
        if (end == std::string::npos) break;
        pos = end + 1;
    }
    if (result.empty()) throw std::invalid_argument("empty peers");
    std::sort(result.begin(), result.end(), [](const auto& a, const auto& b) {
        return a.id < b.id;
    });
    for (size_t i = 1; i < result.size(); ++i)
        if (result[i].id == result[i-1].id)
            throw std::invalid_argument("duplicate peer id");
    return result;
}

struct Transport::Impl {
    struct Connection {
        ibv_qp* qp = nullptr;
        std::vector<Message*> receives;
        uint32_t psn = 0;
        uint32_t unsignaled_posted = 0;
        Bootstrap remote{};
    };
    Config cfg;
    ibv_context* ctx = nullptr;
    ibv_pd* pd = nullptr;
    ibv_cq* cq = nullptr;
    ibv_cq* replication_cq = nullptr;
    ibv_mr* mr = nullptr;
    void* mem = nullptr;
    uint16_t lid = 0;
    std::map<NodeId, Connection> connections;
    size_t recv_next = 0x40000;
    size_t scratch_next = 0;
    bool connected = false;
    explicit Impl(Config c) : cfg(std::move(c)) {
        if (!cfg.self || cfg.self > 255 || cfg.memory_bytes < kScratchOffset + 4096)
            throw std::invalid_argument("invalid local ID or registered memory size");
        scratch_next = cfg.memory_bytes - 16ULL * 1024 * 1024;
        if (cfg.receive_depth < 2 || cfg.receive_depth > 1024)
            throw std::invalid_argument("receive depth must be 2..1024");
        int count = 0;
        ibv_device** devices = ibv_get_device_list(&count);
        if (!devices) throw std::runtime_error("ibv_get_device_list failed");
        for (int i = 0; i < count; ++i) {
            if (!cfg.device.empty() && cfg.device != ibv_get_device_name(devices[i])) continue;
            ibv_context* candidate = ibv_open_device(devices[i]);
            if (!candidate) continue;
            ibv_port_attr attr{};
            if (!ibv_query_port(candidate, cfg.ib_port, &attr) &&
                attr.state == IBV_PORT_ACTIVE && attr.link_layer == IBV_LINK_LAYER_INFINIBAND &&
                attr.lid != 0) {
                ctx = candidate; lid = attr.lid; break;
            }
            ibv_close_device(candidate);
        }
        ibv_free_device_list(devices);
        if (!ctx) throw std::runtime_error("no active InfiniBand port found");
        pd = ibv_alloc_pd(ctx);
        check(pd != nullptr, "ibv_alloc_pd");
        cq = ibv_create_cq(ctx, 8192, nullptr, nullptr, 0);
        check(cq != nullptr, "ibv_create_cq");
        if (!cfg.replication_peers.empty()) {
            replication_cq = ibv_create_cq(ctx, 8192, nullptr, nullptr, 0);
            check(replication_cq != nullptr, "ibv_create_cq replication");
        }
        check(posix_memalign(&mem, 4096, align_up(cfg.memory_bytes, 4096)) == 0, "posix_memalign");
        std::memset(mem, 0, cfg.memory_bytes);
        mr = ibv_reg_mr(pd, mem, cfg.memory_bytes, IBV_ACCESS_LOCAL_WRITE |
                        IBV_ACCESS_REMOTE_WRITE | IBV_ACCESS_REMOTE_READ |
                        IBV_ACCESS_REMOTE_ATOMIC);
        if (!mr) {
            const int err = errno;
            std::string detail = "RDMA: ibv_reg_mr: errno=" + std::to_string(err) +
                                 " (" + std::strerror(err) + ")" +
                                 "; registering " + std::to_string(cfg.memory_bytes) +
                                 " bytes";
            rlimit limit{};
            if (getrlimit(RLIMIT_MEMLOCK, &limit) == 0) {
                auto fmt = [](rlim_t n) {
                    return n == RLIM_INFINITY ? std::string("unlimited") :
                           std::to_string(static_cast<unsigned long long>(n));
                };
                detail += "; memlock soft=" + fmt(limit.rlim_cur) +
                          " hard=" + fmt(limit.rlim_max) + " bytes";
            }
            throw std::runtime_error(detail +
                "; check the process memlock limit and available RDMA resources");
        }
        for (const auto& peer : cfg.peers) {
            if (peer.id == cfg.self) continue;
            auto& conn = connections[peer.id];
            ibv_qp_init_attr attr{};
            const bool repl = std::find(cfg.replication_peers.begin(),
                cfg.replication_peers.end(), peer.id) != cfg.replication_peers.end();
            attr.send_cq = repl ? replication_cq : cq;
            attr.recv_cq = repl ? replication_cq : cq;
            attr.qp_type = IBV_QPT_RC;
            attr.cap.max_send_wr = kMaxSend;
            attr.cap.max_recv_wr = cfg.receive_depth;
            attr.cap.max_send_sge = 1; attr.cap.max_recv_sge = 1;
            attr.cap.max_inline_data = 128;
            conn.qp = ibv_create_qp(pd, &attr);
            check(conn.qp != nullptr, "ibv_create_qp");
            conn.psn = static_cast<uint32_t>((0x2345 + cfg.self * 7919 + peer.id * 131) & 0xffffff);
            ibv_qp_attr init{};
            init.qp_state = IBV_QPS_INIT; init.pkey_index = 0;
            init.port_num = cfg.ib_port;
            init.qp_access_flags = IBV_ACCESS_REMOTE_READ | IBV_ACCESS_REMOTE_WRITE |
                                   IBV_ACCESS_REMOTE_ATOMIC;
            check(!ibv_modify_qp(conn.qp, &init,
                  IBV_QP_STATE | IBV_QP_PKEY_INDEX | IBV_QP_PORT | IBV_QP_ACCESS_FLAGS),
                  "modify QP INIT");
            for (uint32_t slot = 0; slot < cfg.receive_depth; ++slot) {
                recv_next = align_up(recv_next, alignof(Message));
                if (recv_next + sizeof(Message) > kLogOffset)
                    throw std::runtime_error("receive ring exceeds reserved memory");
                auto* buffer = reinterpret_cast<Message*>(static_cast<char*>(mem) + recv_next);
                recv_next += sizeof(Message);
                conn.receives.push_back(buffer);
                post_recv(peer.id, slot);
            }
        }
    }
    ~Impl() {
        for (auto& [id, conn] : connections) if (conn.qp) ibv_destroy_qp(conn.qp);
        if (mr) ibv_dereg_mr(mr);
        if (mem) std::free(mem);
        if (replication_cq) ibv_destroy_cq(replication_cq);
        if (cq) ibv_destroy_cq(cq);
        if (pd) ibv_dealloc_pd(pd);
        if (ctx) ibv_close_device(ctx);
    }
    Connection& conn(NodeId peer) {
        auto it = connections.find(peer);
        if (it == connections.end()) throw std::invalid_argument("unknown RDMA peer");
        return it->second;
    }
    void post_recv(NodeId peer, uint32_t slot) {
        auto& c = conn(peer);
        ibv_sge sge{};
        sge.addr = reinterpret_cast<uintptr_t>(c.receives.at(slot));
        sge.length = sizeof(Message); sge.lkey = mr->lkey;
        ibv_recv_wr wr{}, *bad = nullptr;
        wr.wr_id = encode(Tag::Receive, peer, slot);
        wr.sg_list = &sge; wr.num_sge = 1;
        check(!ibv_post_recv(c.qp, &wr, &bad), "post receive");
    }
    void connect_qp(Connection& c, const Bootstrap& b) {
        ibv_qp_attr rtr{};
        rtr.qp_state = IBV_QPS_RTR;
        rtr.path_mtu = IBV_MTU_1024;
        rtr.dest_qp_num = b.qpn;
        rtr.rq_psn = b.psn;
        rtr.max_dest_rd_atomic = 1;
        rtr.min_rnr_timer = 12;
        rtr.ah_attr.is_global = 0;
        rtr.ah_attr.dlid = static_cast<uint16_t>(b.lid);
        rtr.ah_attr.sl = 0;
        rtr.ah_attr.src_path_bits = 0;
        rtr.ah_attr.port_num = cfg.ib_port;
        check(!ibv_modify_qp(c.qp, &rtr, IBV_QP_STATE | IBV_QP_AV | IBV_QP_PATH_MTU |
              IBV_QP_DEST_QPN | IBV_QP_RQ_PSN | IBV_QP_MAX_DEST_RD_ATOMIC |
              IBV_QP_MIN_RNR_TIMER), "modify QP RTR");
        ibv_qp_attr rts{};
        rts.qp_state = IBV_QPS_RTS;
        rts.timeout = 14; rts.retry_cnt = 7; rts.rnr_retry = 7;
        rts.sq_psn = c.psn; rts.max_rd_atomic = 1;
        check(!ibv_modify_qp(c.qp, &rts, IBV_QP_STATE | IBV_QP_TIMEOUT |
              IBV_QP_RETRY_CNT | IBV_QP_RNR_RETRY | IBV_QP_SQ_PSN |
              IBV_QP_MAX_QP_RD_ATOMIC), "modify QP RTS");
    }
    void handshake(int fd, NodeId expected) {
        auto& c = conn(expected);
        Bootstrap mine{kMagic, kWireVersion, cfg.self, c.qp->qp_num,
                       c.psn, mr->rkey, lid, 0,
                       reinterpret_cast<uint64_t>(mem), cfg.memory_bytes};
        Bootstrap theirs{};
        send_all(fd, &mine, sizeof(mine));
        read_all(fd, &theirs, sizeof(theirs));
        if (theirs.magic != kMagic || theirs.version != kWireVersion ||
            theirs.node != expected || theirs.bytes < kScratchOffset + 4096 ||
            theirs.lid == 0)
            throw std::runtime_error("invalid RDMA bootstrap from peer");
        c.remote = theirs;
        connect_qp(c, theirs);
    }
    bool is_local(const void* p, size_t bytes) const {
        uintptr_t addr = reinterpret_cast<uintptr_t>(p);
        uintptr_t start = reinterpret_cast<uintptr_t>(mem);
        return addr >= start && bytes <= cfg.memory_bytes && addr - start <= cfg.memory_bytes - bytes;
    }
    void* at(size_t offset, size_t length) const {
        if (offset > cfg.memory_bytes || length > cfg.memory_bytes - offset)
            throw std::out_of_range("registered memory offset out of range");
        return static_cast<char*>(mem) + offset;
    }
    void verify_remote(NodeId peer, size_t offset, size_t length) {
        auto& c = conn(peer);
        if (!connected || offset > c.remote.bytes || length > c.remote.bytes - offset)
            throw std::runtime_error("remote memory offset out of range or not connected");
    }
};

Transport::Transport(Config cfg) : impl_(std::make_unique<Impl>(std::move(cfg))) {}
Transport::~Transport() = default;
void Transport::connect() {
    auto& x = *impl_;
    if (x.connected) throw std::logic_error("RDMA transport already connected");
    // All participants listen, connect to lower IDs, then accept all higher IDs.
    // The ordering avoids a circular startup dependency.
    int listen_fd = listener(x.cfg.tcp_port);
    for (const auto& peer : x.cfg.peers) {
        if (peer.id >= x.cfg.self) continue;
        int fd = dial(peer.ip, x.cfg.tcp_port);
        try {
            const uint32_t self_id = x.cfg.self;
            send_all(fd, &self_id, sizeof(self_id));
            x.handshake(fd, peer.id);
        } catch (...) { ::close(fd); ::close(listen_fd); throw; }
        ::close(fd);
    }
    size_t expected = 0;
    for (const auto& peer : x.cfg.peers) if (peer.id > x.cfg.self) ++expected;
    while (expected > 0) {
        sockaddr_in addr{};
        socklen_t len = sizeof(addr);
        int fd = ::accept(listen_fd, reinterpret_cast<sockaddr*>(&addr), &len);
        check(fd >= 0, "TCP accept");
        // The accepted peer identifies itself before QP metadata exchange.
        uint32_t incoming = 0;
        read_all(fd, &incoming, sizeof(incoming));
        try {
            if (incoming <= x.cfg.self || !x.connections.contains(incoming) ||
                x.conn(incoming).remote.magic == kMagic)
                throw std::runtime_error("unexpected incoming peer");
            x.handshake(fd, incoming);
        } catch (...) { ::close(fd); ::close(listen_fd); throw; }
        ::close(fd);
        --expected;
    }
    ::close(listen_fd);
    x.connected = true;
}
void Transport::send(NodeId peer, const Message& msg, WorkId id, bool signal) {
    auto& x = *impl_; auto& c = x.conn(peer);
    if (!x.connected) throw std::runtime_error("RDMA send before connect");
    // Signal at least every 16 WRs per QP, even when the application only
    // requests unsignaled traffic. A later signaled CQE retires earlier WRs.
    const bool signaled = signal || c.unsignaled_posted >= 15;
    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(&msg); sge.length = sizeof(msg);
    ibv_send_wr wr{}, *bad = nullptr;
    wr.wr_id = encode(Tag::Send, peer, id);
    wr.sg_list = &sge; wr.num_sge = 1;
    wr.opcode = IBV_WR_SEND;
    wr.send_flags = IBV_SEND_INLINE | (signaled ? IBV_SEND_SIGNALED : 0);
    check(!ibv_post_send(c.qp, &wr, &bad), "post SEND");
    c.unsignaled_posted = signaled ? 0 : c.unsignaled_posted + 1;
}
bool Transport::write(NodeId peer, const void* p, size_t bytes,
                      size_t offset, WorkId id, bool signal) {
    auto& x = *impl_;
    x.verify_remote(peer, offset, bytes);
    if (!x.is_local(p, bytes)) throw std::invalid_argument("WRITE source is not registered");
    auto& c = x.conn(peer);
    const bool signaled = signal || c.unsignaled_posted >= 15;
    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(p); sge.length = static_cast<uint32_t>(bytes);
    sge.lkey = x.mr->lkey;
    ibv_send_wr wr{}, *bad = nullptr;
    wr.wr_id = encode(Tag::Write, peer, id);
    wr.sg_list = &sge; wr.num_sge = 1; wr.opcode = IBV_WR_RDMA_WRITE;
    wr.send_flags = signaled ? IBV_SEND_SIGNALED : 0;
    wr.wr.rdma.remote_addr = c.remote.base + offset;
    wr.wr.rdma.rkey = c.remote.rkey;
    check(!ibv_post_send(c.qp, &wr, &bad), "post WRITE");
    c.unsignaled_posted = signaled ? 0 : c.unsignaled_posted + 1;
    return signaled;
}
bool Transport::write_inline(NodeId peer, uint64_t value, size_t offset,
                             WorkId id, bool signal) {
    auto& x = *impl_;
    if (offset % alignof(uint64_t))
        throw std::invalid_argument("unaligned Mu inline WRITE offset");
    x.verify_remote(peer, offset, sizeof(uint64_t));
    auto& c = x.conn(peer);
    const bool signaled = signal || c.unsignaled_posted >= 15;
    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(&value);
    sge.length = sizeof(value);
    // lkey is not used for IBV_SEND_INLINE.
    ibv_send_wr wr{}, *bad = nullptr;
    wr.wr_id = encode(Tag::Write, peer, id);
    wr.sg_list = &sge; wr.num_sge = 1;
    wr.opcode = IBV_WR_RDMA_WRITE;
    wr.send_flags = IBV_SEND_INLINE | (signaled ? IBV_SEND_SIGNALED : 0);
    wr.wr.rdma.remote_addr = c.remote.base + offset;
    wr.wr.rdma.rkey = c.remote.rkey;
    check(!ibv_post_send(c.qp, &wr, &bad), "post inline WRITE");
    c.unsignaled_posted = signaled ? 0 : c.unsignaled_posted + 1;
    return signaled;
}
void Transport::compare_swap(NodeId peer, uint64_t* result, size_t offset,
                             uint64_t expected, uint64_t desired, WorkId id) {
    auto& x = *impl_;
    x.verify_remote(peer, offset, sizeof(uint64_t));
    if ((offset % 8) || !x.is_local(result, sizeof(uint64_t)) ||
        (reinterpret_cast<uintptr_t>(result) % 8))
        throw std::invalid_argument("CAS result and remote offset must be aligned");
    ibv_sge sge{};
    sge.addr = reinterpret_cast<uintptr_t>(result); sge.length = sizeof(uint64_t);
    sge.lkey = x.mr->lkey;
    ibv_send_wr wr{}, *bad = nullptr;
    wr.wr_id = encode(Tag::Cas, peer, id);
    wr.sg_list = &sge; wr.num_sge = 1;
    wr.opcode = IBV_WR_ATOMIC_CMP_AND_SWP;
    wr.send_flags = IBV_SEND_SIGNALED;
    wr.wr.atomic.remote_addr = x.conn(peer).remote.base + offset;
    wr.wr.atomic.rkey = x.conn(peer).remote.rkey;
    wr.wr.atomic.compare_add = expected; wr.wr.atomic.swap = desired;
    check(!ibv_post_send(x.conn(peer).qp, &wr, &bad), "post CAS");
}
void Transport::poll_into(std::vector<Completion>& result, int batch) {
    poll_on_cq(result, batch, false);
}
void Transport::poll_replication_into(std::vector<Completion>& result, int batch) {
    poll_on_cq(result, batch, true);
}
void Transport::poll_on_cq(std::vector<Completion>& result, int batch, bool replication) {
    auto& x = *impl_;
    if (replication && !x.replication_cq)
        throw std::runtime_error("replication CQ not configured");
    if (batch <= 0 || batch > 4096) throw std::invalid_argument("invalid poll batch");
    // The benchmark always requests 64 CQEs. Avoid allocating this buffer
    // on every empty CQ poll; also reuse the caller's output vector.
    std::array<ibv_wc, 64> local;
    std::vector<ibv_wc> larger;
    ibv_wc* wc = local.data();
    if (batch > static_cast<int>(local.size())) {
        larger.resize(static_cast<size_t>(batch));
        wc = larger.data();
    }
    int n = ibv_poll_cq(replication ? x.replication_cq : x.cq, batch, wc);
    check(n >= 0, "poll CQ");
    result.clear();
    result.reserve(static_cast<size_t>(n));
    for (int i = 0; i < n; ++i) {
        auto& w = wc[i];
        if (w.status != IBV_WC_SUCCESS)
            throw std::runtime_error(std::string("CQ failure: ") + ibv_wc_status_str(w.status));
        NodeId peer = static_cast<NodeId>((w.wr_id >> 48) & 0xff);
        uint64_t token = w.wr_id & kTokenMask;
        Tag tag = static_cast<Tag>(w.wr_id >> 56);
        Completion c{};
        c.peer = peer; c.work_id = token;
        switch (tag) {
        case Tag::Receive: {
            auto& connection = x.conn(peer);
            if (token >= connection.receives.size() || w.byte_len != sizeof(Message))
                throw std::runtime_error("invalid receive completion");
            c.kind = CompletionKind::Receive;
            c.message = *connection.receives[token];
            x.post_recv(peer, static_cast<uint32_t>(token));
            break;
        }
        case Tag::Send: c.kind = CompletionKind::Send; break;
        case Tag::Write: c.kind = CompletionKind::Write; break;
        case Tag::Cas: c.kind = CompletionKind::CompareSwap; break;
        default: throw std::runtime_error("unexpected CQ work ID tag");
        }
        result.push_back(c);
    }
}
std::vector<Completion> Transport::poll(int batch) {
    std::vector<Completion> result;
    poll_into(result, batch);
    return result;
}
void* Transport::at(size_t offset, size_t length) { return impl_->at(offset, length); }
const void* Transport::at(size_t offset, size_t length) const { return impl_->at(offset, length); }
void* Transport::allocate(size_t len, size_t alignment) {
    auto& x = *impl_;
    size_t offset = align_up(x.scratch_next, alignment);
    void* p = x.at(offset, len);
    x.scratch_next = offset + len;
    return p;
}
size_t Transport::memory_bytes() const { return impl_->cfg.memory_bytes; }
NodeId Transport::self() const { return impl_->cfg.self; }
} // namespace lock::rdma
