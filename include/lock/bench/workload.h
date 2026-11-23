#pragma once
#include "lock/types.h"
#include <cstddef>
#include <cstdint>
#include <random>
#include <string>
namespace lock {
class LockPicker {
public:
    LockPicker(size_t locks, std::string distribution, uint64_t seed);
    LockId next();
private:
    std::mt19937_64 rng_;
    std::discrete_distribution<uint32_t> zipf_;
    std::uniform_int_distribution<uint32_t> uniform_;
    bool use_zipf_ = false;
};
} // namespace lock
