#pragma once
#include "lock/types.h"
#include <array>
#include <cstddef>
#include <span>
#include <stdexcept>

namespace lock {
// Exact globally unique request IDs, never a cumulative sequence: earlier
// requests on other locks or lanes may still be active.
constexpr size_t kGcIdsPerMessage = 5;
using GcIds = std::array<RequestId, kGcIdsPerMessage>;
inline Message retirement_gc_message(NodeId sender, size_t lane,
                                     std::span<const RequestId> ids) {
    if (ids.empty() || ids.size() > kGcIdsPerMessage || lane >= 8)
        throw std::invalid_argument("invalid retirement GC batch");
    GcIds words{};
    for (size_t i = 0; i < ids.size(); ++i) {
        if (!ids[i]) throw std::invalid_argument("zero retirement GC ID");
        words[i] = ids[i];
    }
    auto m = message(Kind::RetirementGc, sender);
    m.request = words[0]; m.other = words[1]; m.position = words[2];
    m.deadline_ns = words[3]; m.value = words[4];
    m.flags = static_cast<uint32_t>(ids.size());
    m.lane = static_cast<uint32_t>(lane);
    return m;
}
inline GcIds decode_retirement_gc(const Message& m, NodeId client, size_t lane) {
    if (message_kind(m) != Kind::RetirementGc || !m.flags ||
        m.flags > kGcIdsPerMessage || m.lane != lane || m.lock_id != 0)
        throw std::runtime_error("invalid retirement GC header");
    GcIds ids{m.request, m.other, m.position, m.deadline_ns, m.value};
    for (size_t i = 0; i < ids.size(); ++i) {
        if (i >= m.flags) {
            if (ids[i]) throw std::runtime_error("nonzero GC padding");
            continue;
        }
        if (!ids[i] || request_node(ids[i]) != client || !request_sequence(ids[i]))
            throw std::runtime_error("invalid retirement GC request ID");
        for (size_t j = 0; j < i; ++j)
            if (ids[j] == ids[i]) throw std::runtime_error("duplicate retirement GC ID");
    }
    return ids;
}
} // namespace lock
