#pragma once

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "hash.hpp"

namespace dsw {

class BlockedBloomFilter {
  public:
    static constexpr std::size_t kBlockBits = BLOCKED_BLOOM_BLOCK_BITS;
    static constexpr std::size_t kWordsPerBlock = BLOCKED_BLOOM_WORDS_PER_BLOCK;
    static constexpr std::size_t kWordBits = WORD_BITS;
    static constexpr std::size_t kWordShift = WORD_SHIFT;
    static constexpr std::size_t kWordMask = WORD_MASK;

    struct alignas(64) Block {
        std::array<std::uint64_t, kWordsPerBlock> words{};
    };

    BlockedBloomFilter(
        std::size_t expected_keys,
        std::size_t bits_per_key,
        std::size_t k_hashes
    )
        : num_blocks_(std::max<std::size_t>(
              1U,
              ((expected_keys * bits_per_key) + (kBlockBits - 1U)) / kBlockBits
          )),
          bits_per_key_(bits_per_key),
          k_hashes_(std::max<std::size_t>(1U, k_hashes)),
          blocks_(num_blocks_) {}

    [[nodiscard]] static BlockedBloomFilter build(
        std::span<const std::uint64_t> keys,
        std::size_t bits_per_key,
        std::size_t k_hashes
    ) {
        BlockedBloomFilter filter(keys.size(), bits_per_key, k_hashes);
        for (const std::uint64_t key : keys) {
            filter.insert(key);
        }
        return filter;
    }

    void insert(std::uint64_t key) noexcept {
        Block& block = blocks_[block_index(key)];
        const std::uint64_t h1 = hash_uint64(key, kBloomBlockSeed);
        const std::uint64_t h2 = hash_uint64(key, kBloomStepSeed) | 1ULL;
        for (std::size_t j = 0; j < k_hashes_; ++j) {
            const std::size_t position = probe_position(h1, h2, j);
            block.words[position >> kWordShift] |= (1ULL << (position & kWordMask));
        }
    }

    [[nodiscard]] bool contains(std::uint64_t key) const noexcept {
        const Block& block = blocks_[block_index(key)];
        const std::uint64_t h1 = hash_uint64(key, kBloomBlockSeed);
        const std::uint64_t h2 = hash_uint64(key, kBloomStepSeed) | 1ULL;
        for (std::size_t j = 0; j < k_hashes_; ++j) {
            const std::size_t position = probe_position(h1, h2, j);
            if ((block.words[position >> kWordShift] & (1ULL << (position & kWordMask))) == 0U) {
                return false;
            }
        }
        return true;
    }

    [[nodiscard]] std::size_t memory_bytes() const noexcept {
        return blocks_.size() * sizeof(Block);
    }

    [[nodiscard]] std::size_t num_blocks() const noexcept { return num_blocks_; }
    [[nodiscard]] std::size_t bits_per_key() const noexcept { return bits_per_key_; }
    [[nodiscard]] std::size_t k_hashes() const noexcept { return k_hashes_; }
    [[nodiscard]] std::span<const Block> raw_blocks() const noexcept {
        return blocks_;
    }
    [[nodiscard]] std::vector<std::uint64_t> debug_block_words() const {
        std::vector<std::uint64_t> words;
        words.reserve(blocks_.size() * kWordsPerBlock);
        for (const Block& block : blocks_) {
            words.insert(words.end(), block.words.begin(), block.words.end());
        }
        return words;
    }

  private:
    [[nodiscard]] std::size_t block_index(std::uint64_t key) const noexcept {
        return static_cast<std::size_t>(
            hash_uint64(key, kBloomIndexSeed) % static_cast<std::uint64_t>(num_blocks_)
        );
    }

    [[nodiscard]] std::size_t probe_position(
        std::uint64_t h1,
        std::uint64_t h2,
        std::size_t hash_index
    ) const noexcept {
        return static_cast<std::size_t>(
            (h1 + static_cast<std::uint64_t>(hash_index) * h2) &
            static_cast<std::uint64_t>(kBlockBits - 1U)
        );
    }

    std::size_t num_blocks_;
    std::size_t bits_per_key_;
    std::size_t k_hashes_;
    std::vector<Block> blocks_;
};

[[nodiscard]] inline double empirical_false_positive_rate(
    const BlockedBloomFilter& filter,
    std::span<const std::uint64_t> negatives
) {
    std::size_t false_positives = 0U;
    for (const std::uint64_t key : negatives) {
        false_positives += static_cast<std::size_t>(filter.contains(key));
    }
    return negatives.empty()
               ? 0.0
               : static_cast<double>(false_positives) /
                     static_cast<double>(negatives.size());
}

}  // namespace dsw
