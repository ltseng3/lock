#pragma once
#include <stdexcept>
#include <string>
namespace lock {
enum class Backend { Cas, Mu, Handoff };
inline Backend parse_backend(const std::string& name) {
    if (name == "cas") return Backend::Cas;
    if (name == "mu") return Backend::Mu;
    if (name == "handoff") return Backend::Handoff;
    throw std::invalid_argument("unknown backend: " + name);
}
} // namespace lock
