#pragma once

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <vector>

#include "hash.hpp"

namespace dsw {

class QuotientFilter {
  public:
    struct Fingerprint {
        std::size_t quotient;
        std::uint32_t remainder;
    };

    struct Instrumentation {
        std::size_t insert_count = 0U;
        std::size_t total_probe_distance = 0U;
        std::size_t max_probe_distance = 0U;
        std::size_t total_cluster_length = 0U;
        std::size_t max_cluster_length = 0U;
    };

    QuotientFilter(std::uint32_t capacity_pow2, std::uint32_t remainder_bits)
        : capacity_pow2_(capacity_pow2),
          remainder_bits_(remainder_bits),
          capacity_(1ULL << capacity_pow2_),
          index_mask_(capacity_ - 1U),
          remainder_mask_((1U << remainder_bits_) - 1U),
          occupied_mask_(1U << remainder_bits_),
          continuation_mask_(1U << (remainder_bits_ + 1U)),
          shifted_mask_(1U << (remainder_bits_ + 2U)),
          slots_(capacity_, 0U) {
        scratch_items_.reserve(QUOTIENT_FILTER_CLUSTER_RESERVE);
        scratch_occupied_quotients_.reserve(QUOTIENT_FILTER_CLUSTER_RESERVE);
        scratch_positions_.reserve(QUOTIENT_FILTER_CLUSTER_RESERVE);
    }

    [[nodiscard]] static QuotientFilter create(
        std::uint32_t capacity_pow2,
        std::uint32_t remainder_bits
    ) {
        return QuotientFilter(capacity_pow2, remainder_bits);
    }

    [[nodiscard]] bool insert(std::uint64_t key) {
        if (count_ >= capacity_) {
            return false;
        }

        const Fingerprint fingerprint = split_fingerprint(key);
        if (contains_fingerprint(fingerprint.quotient, fingerprint.remainder)) {
            return false;
        }

        if (slot_empty(fingerprint.quotient)) {
            slots_[fingerprint.quotient] = make_slot(
                fingerprint.remainder,
                true,
                false,
                false
            );
            ++count_;
            record_insert_stats(0U, 1U);
            return true;
        }

        const std::size_t cluster_start = find_cluster_start(fingerprint.quotient);
        materialize_cluster(cluster_start);
        const std::size_t insert_position =
            find_insert_position(
                scratch_items_,
                cluster_start,
                fingerprint.quotient,
                fingerprint.remainder
            );
        scratch_items_.reserve(scratch_items_.size() + 1U);
        scratch_items_.push_back(Entry{fingerprint.quotient, fingerprint.remainder});
        for (std::size_t index = scratch_items_.size() - 1U; index > insert_position; --index) {
            scratch_items_[index] = scratch_items_[index - 1U];
        }
        scratch_items_[insert_position] = Entry{fingerprint.quotient, fingerprint.remainder};

        const std::size_t cluster_length = scratch_items_.size();
        rewrite_cluster(cluster_start, cluster_length);
        scratch_items_.clear();
        scratch_occupied_quotients_.clear();
        scratch_positions_.clear();
        ++count_;
        const std::size_t physical_slot = advance(cluster_start, insert_position);
        record_insert_stats(distance(fingerprint.quotient, physical_slot), cluster_length);
        return true;
    }

    [[nodiscard]] bool contains(std::uint64_t key) const {
        const Fingerprint fingerprint = split_fingerprint(key);
        return contains_fingerprint(fingerprint.quotient, fingerprint.remainder);
    }

    [[nodiscard]] bool erase(std::uint64_t key) {
        const Fingerprint fingerprint = split_fingerprint(key);
        if (!occupied(fingerprint.quotient)) {
            return false;
        }

        const std::size_t cluster_start = find_cluster_start(fingerprint.quotient);
        materialize_cluster(cluster_start);
        const std::size_t clear_span = scratch_items_.size();
        const auto match = std::find_if(
            scratch_items_.begin(),
            scratch_items_.end(),
            [fingerprint = fingerprint](const Entry& entry) {
                return entry.quotient == fingerprint.quotient &&
                       entry.remainder == fingerprint.remainder;
            }
        );
        if (match == scratch_items_.end()) {
            scratch_items_.clear();
            scratch_occupied_quotients_.clear();
            scratch_positions_.clear();
            return false;
        }

        const std::size_t match_index =
            static_cast<std::size_t>(match - scratch_items_.begin());
        for (std::size_t index = match_index; index + 1U < scratch_items_.size(); ++index) {
            scratch_items_[index] = scratch_items_[index + 1U];
        }
        scratch_items_.pop_back();
        rewrite_cluster(cluster_start, clear_span);
        scratch_items_.clear();
        scratch_occupied_quotients_.clear();
        scratch_positions_.clear();
        --count_;
        return true;
    }

    [[nodiscard]] double load_factor() const {
        return capacity_ == 0U
                   ? 0.0
                   : static_cast<double>(count_) / static_cast<double>(capacity_);
    }

    [[nodiscard]] std::size_t memory_bytes() const {
        return slots_.size() * sizeof(std::uint32_t);
    }

    [[nodiscard]] std::size_t size() const { return count_; }
    [[nodiscard]] std::size_t capacity() const { return capacity_; }
    [[nodiscard]] std::uint32_t remainder_bits() const { return remainder_bits_; }
    [[nodiscard]] double average_probe_distance() const {
        return instrumentation_.insert_count == 0U
                   ? 0.0
                   : static_cast<double>(instrumentation_.total_probe_distance) /
                         static_cast<double>(instrumentation_.insert_count);
    }
    [[nodiscard]] double average_cluster_length() const {
        return instrumentation_.insert_count == 0U
                   ? 0.0
                   : static_cast<double>(instrumentation_.total_cluster_length) /
                         static_cast<double>(instrumentation_.insert_count);
    }
    [[nodiscard]] std::size_t max_probe_distance() const {
        return instrumentation_.max_probe_distance;
    }
    [[nodiscard]] std::size_t max_cluster_length() const {
        return instrumentation_.max_cluster_length;
    }
    [[nodiscard]] const Instrumentation& instrumentation() const { return instrumentation_; }
    [[nodiscard]] std::span<const std::uint32_t> raw_slots() const { return slots_; }

  private:
    struct Entry {
        std::size_t quotient;
        std::uint32_t remainder;
    };

    [[nodiscard]] Fingerprint split_fingerprint(
        std::uint64_t key
    ) const {
        const std::uint64_t fingerprint = hash_uint64(key);
        const std::size_t quotient =
            static_cast<std::size_t>(fingerprint & static_cast<std::uint64_t>(index_mask_));
        const std::uint32_t remainder_value = static_cast<std::uint32_t>(
            (fingerprint >> capacity_pow2_) & static_cast<std::uint64_t>(remainder_mask_)
        );
        return Fingerprint{quotient, remainder_value};
    }

    [[nodiscard]] std::size_t advance(std::size_t index, std::size_t steps) const {
        return (index + steps) & index_mask_;
    }

    [[nodiscard]] std::size_t decrement(std::size_t index) const {
        return (index - 1U) & index_mask_;
    }

    [[nodiscard]] std::size_t increment(std::size_t index) const {
        return (index + 1U) & index_mask_;
    }

    [[nodiscard]] std::size_t distance(std::size_t start, std::size_t finish) const {
        return (finish + capacity_ - start) & index_mask_;
    }

    [[nodiscard]] bool slot_empty(std::size_t index) const {
        return slots_[index] == 0U;
    }

    [[nodiscard]] bool occupied(std::size_t index) const {
        return (slots_[index] & occupied_mask_) != 0U;
    }

    [[nodiscard]] bool continuation(std::size_t index) const {
        return (slots_[index] & continuation_mask_) != 0U;
    }

    [[nodiscard]] bool shifted(std::size_t index) const {
        return (slots_[index] & shifted_mask_) != 0U;
    }

    [[nodiscard]] std::uint32_t remainder(std::size_t index) const {
        return slots_[index] & remainder_mask_;
    }

    [[nodiscard]] std::uint32_t make_slot(
        std::uint32_t remainder_value,
        bool occupied_bit,
        bool continuation_bit,
        bool shifted_bit
    ) const {
        return remainder_value | (occupied_bit ? occupied_mask_ : 0U) |
               (continuation_bit ? continuation_mask_ : 0U) |
               (shifted_bit ? shifted_mask_ : 0U);
    }

    [[nodiscard]] std::size_t next_occupied(std::size_t quotient) const {
        std::size_t cursor = increment(quotient);
        while (!occupied(cursor)) {
            cursor = increment(cursor);
        }
        return cursor;
    }

    [[nodiscard]] std::size_t find_cluster_start(std::size_t index) const {
        std::size_t cursor = index;
        while (!slot_empty(decrement(cursor))) {
            cursor = decrement(cursor);
        }
        return cursor;
    }

    [[nodiscard]] std::size_t find_run_start(std::size_t quotient) const {
        std::size_t cluster_start = find_cluster_start(quotient);
        std::size_t run_start = cluster_start;
        std::size_t run_quotient = cluster_start;

        while (run_quotient != quotient) {
            do {
                run_start = increment(run_start);
            } while (!slot_empty(run_start) && continuation(run_start));
            run_quotient = next_occupied(run_quotient);
        }

        return run_start;
    }

    [[nodiscard]] bool contains_fingerprint(
        std::size_t quotient,
        std::uint32_t remainder_value
    ) const {
        if (!occupied(quotient)) {
            return false;
        }

        std::size_t cursor = find_run_start(quotient);
        while (true) {
            const std::uint32_t current = remainder(cursor);
            if (current == remainder_value) {
                return true;
            }
            if (current > remainder_value) {
                return false;
            }

            const std::size_t next = increment(cursor);
            if (slot_empty(next) || !continuation(next)) {
                return false;
            }
            cursor = next;
        }
    }

    void materialize_cluster(std::size_t cluster_start) {
        scratch_items_.clear();
        if (slot_empty(cluster_start)) {
            return;
        }

        std::size_t cursor = cluster_start;
        std::size_t current_quotient = cluster_start;
        bool first = true;
        while (!slot_empty(cursor)) {
            if (!first && !continuation(cursor)) {
                current_quotient = next_occupied(current_quotient);
            }
            scratch_items_.push_back(Entry{current_quotient, remainder(cursor)});
            cursor = increment(cursor);
            first = false;
        }
    }

    [[nodiscard]] std::size_t find_insert_position(
        const std::vector<Entry>& items,
        std::size_t cluster_start,
        std::size_t quotient,
        std::uint32_t remainder_value
    ) const {
        const std::size_t target_rank = distance(cluster_start, quotient);
        for (std::size_t index = 0; index < items.size(); ++index) {
            const std::size_t item_rank = distance(cluster_start, items[index].quotient);
            if (item_rank > target_rank) {
                return index;
            }
            if (item_rank == target_rank && items[index].remainder >= remainder_value) {
                return index;
            }
        }
        return items.size();
    }

    void rewrite_cluster(std::size_t clear_start, std::size_t clear_span) {
        const std::vector<Entry>& items = scratch_items_;
        for (std::size_t index = 0; index < clear_span; ++index) {
            slots_[advance(clear_start, index)] = 0U;
        }
        if (items.empty()) {
            return;
        }

        scratch_occupied_quotients_.clear();
        for (const Entry& entry : items) {
            if (
                scratch_occupied_quotients_.empty() ||
                scratch_occupied_quotients_.back() != entry.quotient
            ) {
                scratch_occupied_quotients_.push_back(entry.quotient);
            }
        }

        scratch_positions_.clear();
        scratch_positions_.resize(items.size(), 0U);
        std::size_t next_free_rank = 0U;
        for (std::size_t index = 0; index < items.size(); ++index) {
            const std::size_t quotient_rank = distance(clear_start, items[index].quotient);
            const std::size_t position_rank = std::max(next_free_rank, quotient_rank);
            scratch_positions_[index] = advance(clear_start, position_rank);
            next_free_rank = position_rank + 1U;
        }

        std::size_t occupied_cursor = 0U;
        for (std::size_t index = 0; index < items.size(); ++index) {
            const std::size_t position = scratch_positions_[index];
            const bool occupied_bit =
                occupied_cursor < scratch_occupied_quotients_.size() &&
                scratch_occupied_quotients_[occupied_cursor] == position;
            if (occupied_bit) {
                ++occupied_cursor;
            }
            const bool continuation_bit =
                index > 0U && items[index].quotient == items[index - 1U].quotient;
            const bool shifted_bit = position != items[index].quotient;
            slots_[position] = make_slot(
                items[index].remainder,
                occupied_bit,
                continuation_bit,
                shifted_bit
            );
        }
    }

    void record_insert_stats(std::size_t probe_distance, std::size_t cluster_length) {
        ++instrumentation_.insert_count;
        instrumentation_.total_probe_distance += probe_distance;
        instrumentation_.max_probe_distance =
            std::max(instrumentation_.max_probe_distance, probe_distance);
        instrumentation_.total_cluster_length += cluster_length;
        instrumentation_.max_cluster_length =
            std::max(instrumentation_.max_cluster_length, cluster_length);
    }

    std::uint32_t capacity_pow2_{};
    std::uint32_t remainder_bits_{};
    std::size_t capacity_{};
    std::size_t index_mask_{};
    std::uint32_t remainder_mask_{};
    std::uint32_t occupied_mask_{};
    std::uint32_t continuation_mask_{};
    std::uint32_t shifted_mask_{};
    std::size_t count_{0U};
    Instrumentation instrumentation_{};
    std::vector<std::uint32_t> slots_;
    std::vector<Entry> scratch_items_;
    std::vector<std::size_t> scratch_occupied_quotients_;
    std::vector<std::size_t> scratch_positions_;
};

}  // namespace dsw
