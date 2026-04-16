#pragma once

#include <cstddef>
#include <cstdint>

#include "hash_constants.hpp"

namespace dsw {

[[nodiscard]] constexpr std::uint64_t splitmix64(
    std::uint64_t value,
    std::uint64_t seed = 0
) noexcept {
    std::uint64_t z = value + seed + SPLITMIX_INCREMENT;
    z = (z ^ (z >> 30U)) * SPLITMIX_MUL1;
    z = (z ^ (z >> 27U)) * SPLITMIX_MUL2;
    return z ^ (z >> 31U);
}

[[nodiscard]] constexpr std::uint64_t hash_uint64(
    std::uint64_t key,
    std::uint64_t seed = 0
) noexcept {
    return splitmix64(key, seed);
}

[[nodiscard]] constexpr std::size_t recommended_k_hashes(
    std::size_t bits_per_key
) noexcept {
    // Unsigned integer division here matches Mojo's floor division because all
    // operands are non-negative compile-time constants or sizes.
    const std::size_t estimate =
        (bits_per_key * RECOMMENDED_K_NUMERATOR + RECOMMENDED_K_ROUNDING) /
        RECOMMENDED_K_DENOMINATOR;
    return estimate < RECOMMENDED_K_MINIMUM ? RECOMMENDED_K_MINIMUM : estimate;
}

[[nodiscard]] constexpr std::uint64_t dataset_key(std::uint64_t index) noexcept {
    return hash_uint64(index, DATASET_KEY_SEED) & SIGNED_INT_MAX;
}

[[nodiscard]] constexpr std::uint64_t negative_query(std::uint64_t index) noexcept {
    return hash_uint64(index, DATASET_NEGATIVE_SEED) | HIGH_BIT_MASK;
}

}  // namespace dsw
