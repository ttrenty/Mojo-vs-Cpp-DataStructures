#pragma once

#include <bit>
#include <cstddef>
#include <cstdint>
#include <optional>
#include <span>
#include <vector>

#include "hash_constants.hpp"

namespace dsw {

class EliasFano {
  public:
    static constexpr std::size_t kWordBits = WORD_BITS;
    static constexpr std::size_t kWordShift = WORD_SHIFT;
    static constexpr std::size_t kWordMask = WORD_MASK;

    EliasFano() = default;

    static EliasFano build(
        std::span<const std::uint64_t> sorted_unique_values,
        std::uint64_t universe_max
    ) {
        EliasFano encoded;
        encoded.count_ = sorted_unique_values.size();
        encoded.universe_max_ = universe_max;
        if (encoded.count_ == 0U) {
            return encoded;
        }

        const std::uint64_t ratio = std::max<std::uint64_t>(
            1U,
            universe_max / static_cast<std::uint64_t>(encoded.count_)
        );
        const auto ratio_width = static_cast<std::size_t>(std::bit_width(ratio));
        encoded.lower_bits_ = ratio_width > 0U ? ratio_width - 1U : 0U;
        encoded.lower_mask_ = encoded.lower_bits_ == 0U
                                  ? 0U
                                  : ((1ULL << encoded.lower_bits_) - 1ULL);

        const std::size_t lower_total_bits = encoded.count_ * encoded.lower_bits_;
        encoded.lower_words_.assign((lower_total_bits + kWordMask) / kWordBits, 0U);

        const std::uint64_t upper_bound = (universe_max >> encoded.lower_bits_) + encoded.count_;
        encoded.upper_bit_length_ = static_cast<std::size_t>(upper_bound + 1U);
        encoded.upper_words_.assign((encoded.upper_bit_length_ + kWordMask) / kWordBits, 0U);

        for (std::size_t index = 0; index < sorted_unique_values.size(); ++index) {
            const std::uint64_t value = sorted_unique_values[index];
            encoded.write_lower(index, value & encoded.lower_mask_);
            const std::uint64_t upper = value >> encoded.lower_bits_;
            const std::size_t upper_position = static_cast<std::size_t>(upper + index);
            encoded.upper_words_[upper_position >> kWordShift] |=
                1ULL << (upper_position & kWordMask);
        }

        return encoded;
    }

    [[nodiscard]] bool contains(std::uint64_t value) const {
        const auto candidate = predecessor(value);
        return candidate.has_value() && candidate.value() == value;
    }

    [[nodiscard]] std::uint64_t select(std::size_t index) const {
        std::size_t remaining = index;
        for (std::size_t word_index = 0; word_index < upper_words_.size(); ++word_index) {
            std::uint64_t word = upper_words_[word_index];
            const std::size_t bit_count = static_cast<std::size_t>(std::popcount(word));
            if (remaining >= bit_count) {
                remaining -= bit_count;
                continue;
            }
            while (remaining > 0U) {
                word &= (word - 1U);
                --remaining;
            }
            const std::size_t bit_index = static_cast<std::size_t>(std::countr_zero(word));
            const std::size_t position = word_index * kWordBits + bit_index;
            const std::uint64_t upper = static_cast<std::uint64_t>(position - index);
            return (upper << lower_bits_) | read_lower(index);
        }
        return 0U;
    }

    [[nodiscard]] std::optional<std::uint64_t> predecessor(std::uint64_t value) const {
        if (count_ == 0U) {
            return std::nullopt;
        }

        std::size_t left = 0U;
        std::size_t right = count_;
        while (left < right) {
            const std::size_t middle = (left + right) / 2U;
            if (select(middle) <= value) {
                left = middle + 1U;
            } else {
                right = middle;
            }
        }
        if (left == 0U) {
            return std::nullopt;
        }
        return select(left - 1U);
    }

    [[nodiscard]] std::size_t memory_bytes() const {
        return (lower_words_.size() + upper_words_.size()) * sizeof(std::uint64_t);
    }

    [[nodiscard]] std::size_t size() const { return count_; }
    [[nodiscard]] std::size_t lower_bits() const { return lower_bits_; }
    [[nodiscard]] std::uint64_t universe_max() const { return universe_max_; }
    [[nodiscard]] std::span<const std::uint64_t> lower_words() const { return lower_words_; }
    [[nodiscard]] std::span<const std::uint64_t> upper_words() const { return upper_words_; }

  private:
    void write_lower(std::size_t index, std::uint64_t lower_value) {
        if (lower_bits_ == 0U) {
            return;
        }
        const std::size_t offset = index * lower_bits_;
        const std::size_t word_index = offset >> kWordShift;
        const std::size_t bit_offset = offset & kWordMask;
        lower_words_[word_index] |= lower_value << bit_offset;
        if (bit_offset + lower_bits_ > kWordBits) {
            lower_words_[word_index + 1U] |= lower_value >> (kWordBits - bit_offset);
        }
    }

    [[nodiscard]] std::uint64_t read_lower(std::size_t index) const {
        if (lower_bits_ == 0U) {
            return 0U;
        }
        const std::size_t offset = index * lower_bits_;
        const std::size_t word_index = offset >> kWordShift;
        const std::size_t bit_offset = offset & kWordMask;
        std::uint64_t value = lower_words_[word_index] >> bit_offset;
        if (bit_offset + lower_bits_ > kWordBits) {
            value |= lower_words_[word_index + 1U] << (kWordBits - bit_offset);
        }
        return value & lower_mask_;
    }

    std::size_t count_{0U};
    std::uint64_t universe_max_{0U};
    std::size_t lower_bits_{0U};
    std::uint64_t lower_mask_{0U};
    std::size_t upper_bit_length_{0U};
    std::vector<std::uint64_t> lower_words_;
    std::vector<std::uint64_t> upper_words_;
};

}  // namespace dsw
