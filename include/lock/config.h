#pragma once
#include <cstdint>
#include <map>
#include <string>
#include <vector>
namespace lock {
class Options {
public:
    Options(int argc, char** argv);
    bool has(const std::string& key) const;
    std::string get(const std::string& key, const std::string& fallback = "") const;
    std::string require(const std::string& key) const;
    uint64_t number(const std::string& key, uint64_t fallback) const;
    double real(const std::string& key, double fallback) const;
    std::vector<std::string> list(const std::string& key) const;
private:
    std::map<std::string, std::string> entries_;
};
} // namespace lock
