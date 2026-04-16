#pragma once

#include <algorithm>
#include <bit>
#include <chrono>
#include <cmath>
#include <cstdint>
#include <cstdlib>
#include <numeric>
#include <span>
#include <string>
#include <string_view>
#include <vector>

#include "hash.hpp"

#if defined(__linux__)
#include <sched.h>
#include <unistd.h>
#endif

namespace dsw {

struct SummaryStats {
    double min = 0.0;
    double median = 0.0;
    double mean = 0.0;
    double max = 0.0;
    double stddev = 0.0;
};

template <typename Fn>
[[nodiscard]] inline std::uint64_t measure_ns(Fn&& fn) {
    const auto start = std::chrono::steady_clock::now();
    fn();
    const auto finish = std::chrono::steady_clock::now();
    return static_cast<std::uint64_t>(
        std::chrono::duration_cast<std::chrono::nanoseconds>(finish - start).count()
    );
}

[[nodiscard]] inline SummaryStats summarize(std::vector<double> samples) {
    std::sort(samples.begin(), samples.end());
    SummaryStats stats{};
    stats.min = samples.front();
    stats.max = samples.back();
    stats.median = samples[samples.size() / 2U];
    stats.mean = std::accumulate(samples.begin(), samples.end(), 0.0) /
                 static_cast<double>(samples.size());
    double variance = 0.0;
    for (const double sample : samples) {
        const double delta = sample - stats.mean;
        variance += delta * delta;
    }
    variance /= static_cast<double>(samples.size());
    stats.stddev = std::sqrt(variance);
    return stats;
}

inline void do_not_optimize(std::size_t value) {
#if defined(__clang__) || defined(__GNUC__)
    asm volatile("" : : "r,m"(value) : "memory");
#else
    (void)value;
#endif
}

inline void do_not_optimize_u64(std::uint64_t value) {
#if defined(__clang__) || defined(__GNUC__)
    asm volatile("" : : "r,m"(value) : "memory");
#else
    (void)value;
#endif
}

[[nodiscard]] inline bool pin_process_to_core(int core) {
#if defined(__linux__)
    cpu_set_t set{};
    CPU_ZERO(&set);
    CPU_SET(core, &set);
    return sched_setaffinity(0, sizeof(set), &set) == 0;
#else
    (void)core;
    return false;
#endif
}

[[nodiscard]] inline std::vector<std::uint64_t> make_deterministic_keys(
    std::size_t count,
    std::uint64_t seed,
    bool set_high_bit
) {
    std::vector<std::uint64_t> values;
    values.reserve(count);
    for (std::size_t index = 0; index < count; ++index) {
        std::uint64_t value = hash_uint64(index, seed);
        value = set_high_bit ? (value | HIGH_BIT_MASK) : (value & ~HIGH_BIT_MASK);
        values.push_back(value);
    }
    return values;
}

[[nodiscard]] inline std::vector<std::uint64_t> default_positive_keys(
    std::size_t count
) {
    std::vector<std::uint64_t> values;
    values.reserve(count);
    for (std::size_t index = 0; index < count; ++index) {
        values.push_back(dataset_key(index));
    }
    return values;
}

[[nodiscard]] inline std::vector<std::uint64_t> default_negative_queries(
    std::size_t count
) {
    std::vector<std::uint64_t> values;
    values.reserve(count);
    for (std::size_t index = 0; index < count; ++index) {
        values.push_back(negative_query(index));
    }
    return values;
}

[[nodiscard]] inline std::vector<std::uint64_t> make_mixed_queries(
    std::span<const std::uint64_t> positives,
    std::span<const std::uint64_t> negatives
) {
    const std::size_t count = std::min(positives.size(), negatives.size());
    std::vector<std::uint64_t> queries;
    queries.reserve(count * 2U);
    for (std::size_t index = 0; index < count; ++index) {
        queries.push_back(positives[index]);
        queries.push_back(negatives[index]);
    }
    return queries;
}

[[nodiscard]] inline std::vector<std::uint64_t> make_sorted_unique_values(
    std::size_t count,
    std::uint64_t universe_factor
) {
    const std::uint64_t stride = std::max<std::uint64_t>(1U, universe_factor);
    std::vector<std::uint64_t> values;
    values.reserve(count);
    for (std::size_t index = 0; index < count; ++index) {
        const std::uint64_t jitter = hash_uint64(index, kDatasetKeySeed) % stride;
        values.push_back(static_cast<std::uint64_t>(index) * stride + jitter);
    }
    return values;
}

[[nodiscard]] inline std::string density_label(std::uint64_t universe_factor) {
    if (universe_factor <= DENSE_UNIVERSE_FACTOR_MAX) {
        return "dense";
    }
    if (universe_factor <= MEDIUM_UNIVERSE_FACTOR_MAX) {
        return "medium";
    }
    return "sparse";
}

[[nodiscard]] inline std::string compiler_version_string() {
#if defined(__clang__)
    return std::string("clang ") + __clang_version__;
#elif defined(__GNUC__)
    return std::string("gcc ") + __VERSION__;
#elif defined(_MSC_VER)
    return "msvc";
#else
    return "unknown";
#endif
}

[[nodiscard]] inline std::string env_or_default(
    const char* name,
    std::string_view fallback
) {
    if (const char* value = std::getenv(name)) {
        return value;
    }
    return std::string(fallback);
}

}  // namespace dsw
