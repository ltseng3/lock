#pragma once
#include "lock/types.h"
#include <cstddef>
#include <deque>
#include <vector>
namespace lock {
struct Action {
    NodeId destination;
    Message message;
};
// Reference FIFO semantics: a grant is emitted only after an ordered mutation commits.
class MuState {
public:
    explicit MuState(size_t lock_count);
    std::vector<Action> apply(const Message& committed);
    size_t lock_count() const { return locks_.size(); }
private:
    struct State {
        RequestId holder = 0;
        std::deque<RequestId> waiting;
    };
    std::vector<State> locks_;
};
} // namespace lock
