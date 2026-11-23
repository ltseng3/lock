#include "lock/config.h"
#include <cmath>
#include <sstream>
#include <stdexcept>
namespace lock {
Options::Options(int argc, char** argv) {
    for (int i = 1; i < argc; ++i) {
        std::string arg(argv[i]);
        if (!arg.starts_with("--") || arg.find('=') == std::string::npos)
            throw std::invalid_argument("expected --name=value, got: " + arg);
        auto equal = arg.find('=');
        auto key = arg.substr(2, equal - 2);
        if (key.empty() || !entries_.emplace(key, arg.substr(equal + 1)).second)
            throw std::invalid_argument("duplicate or empty option: " + arg);
    }
}
bool Options::has(const std::string& key) const { return entries_.contains(key); }
std::string Options::get(const std::string& key, const std::string& fallback) const {
    auto it = entries_.find(key);
    return it == entries_.end() ? fallback : it->second;
}
std::string Options::require(const std::string& key) const {
    if (!has(key) || get(key).empty()) throw std::invalid_argument("missing --" + key);
    return get(key);
}
uint64_t Options::number(const std::string& key, uint64_t fallback) const {
    if (!has(key)) return fallback;
    size_t used = 0;
    uint64_t result = std::stoull(require(key), &used);
    if (used != get(key).size()) throw std::invalid_argument("invalid number: " + key);
    return result;
}
double Options::real(const std::string& key, double fallback) const {
    if (!has(key)) return fallback;
    size_t used = 0;
    double result = std::stod(require(key), &used);
    if (used != get(key).size() || !std::isfinite(result))
        throw std::invalid_argument("invalid real: " + key);
    return result;
}
std::vector<std::string> Options::list(const std::string& key) const {
    std::vector<std::string> result;
    std::istringstream in(get(key));
    std::string item;
    while (std::getline(in, item, ',')) {
        if (item.empty()) throw std::invalid_argument("empty list entry: " + key);
        result.push_back(item);
    }
    return result;
}
} // namespace lock
