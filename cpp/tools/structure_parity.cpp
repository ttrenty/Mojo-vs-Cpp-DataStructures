#include <cstdlib>
#include <iostream>
#include <optional>
#include <string_view>
#include <vector>

#include "benchmark_utils.hpp"
#include "blocked_bloom.hpp"
#include "elias_fano.hpp"
#include "hash.hpp"
#include "quotient_filter.hpp"

namespace {

void emit_text(std::string_view name, std::string_view value) {
    std::cout << name << '=' << value << '\n';
}

void emit_int(std::string_view name, std::size_t value) {
    std::cout << name << '=' << value << '\n';
}

void emit_u64(std::string_view name, std::uint64_t value) {
    std::cout << name << '=' << value << '\n';
}

void emit_u32(std::string_view name, std::uint32_t value) {
    std::cout << name << '=' << value << '\n';
}

void emit_bool(std::string_view name, bool value) {
    std::cout << name << '=' << (value ? 1 : 0) << '\n';
}

void emit_optional_u64(
    std::string_view name,
    const std::optional<std::uint64_t>& value
) {
    if (value.has_value()) {
        emit_u64(name, value.value());
        return;
    }
    emit_text(name, "none");
}

void print_usage() {
    std::cout << "usage: cpp_structure_parity <blocked_bloom|quotient_filter|elias_fano>\n";
}

void emit_blocked_bloom_parity() {
    constexpr std::size_t kBitsPerKey = 10U;
    const std::size_t kHashes = dsw::recommended_k_hashes(kBitsPerKey);
    const auto positives = dsw::default_positive_keys(24U);
    const auto negatives = dsw::default_negative_queries(12U);
    const auto filter = dsw::BlockedBloomFilter::build(positives, kBitsPerKey, kHashes);

    emit_text("structure", "blocked_bloom");
    emit_int("bits_per_key", kBitsPerKey);
    emit_int("k_hashes", kHashes);
    emit_int("memory_bytes", filter.memory_bytes());

    for (const auto word : filter.debug_block_words()) {
        emit_u64("block_word", word);
    }
    for (const auto key : positives) {
        emit_u64("positive_key", key);
        emit_bool("positive_contains", filter.contains(key));
    }
    for (const auto key : negatives) {
        emit_u64("negative_key", key);
        emit_bool("negative_contains", filter.contains(key));
    }
}

void emit_quotient_filter_parity() {
    auto filter = dsw::QuotientFilter::create(6U, 8U);
    const auto positives = dsw::default_positive_keys(32U);
    const auto negatives = dsw::default_negative_queries(8U);
    constexpr std::size_t kEraseIndices[] = {3U, 7U, 11U, 15U};

    emit_text("structure", "quotient_filter");
    emit_int("capacity_pow2", 6U);
    emit_int("remainder_bits", 8U);

    for (std::size_t index = 0; index < 20U; ++index) {
        emit_bool("insert_result", filter.insert(positives[index]));
    }
    for (const auto erase_index : kEraseIndices) {
        emit_u64("erase_key", positives[erase_index]);
        emit_bool("erase_result", filter.erase(positives[erase_index]));
    }
    for (std::size_t index = 20U; index < 28U; ++index) {
        emit_bool("insert_result", filter.insert(positives[index]));
    }

    const auto& instrumentation = filter.instrumentation();
    emit_int("count", filter.size());
    emit_int("capacity", filter.capacity());
    emit_int("insert_count", instrumentation.insert_count);
    emit_int("total_probe_distance", instrumentation.total_probe_distance);
    emit_int("max_probe_distance", instrumentation.max_probe_distance);
    emit_int("total_cluster_length", instrumentation.total_cluster_length);
    emit_int("max_cluster_length", instrumentation.max_cluster_length);

    for (const auto slot : filter.raw_slots()) {
        emit_u32("slot", slot);
    }
    for (const auto key : positives) {
        emit_u64("positive_key", key);
        emit_bool("positive_contains", filter.contains(key));
    }
    for (const auto key : negatives) {
        emit_u64("negative_key", key);
        emit_bool("negative_contains", filter.contains(key));
    }
}

void emit_elias_fano_parity() {
    const auto values = dsw::make_sorted_unique_values(64U, 64U);
    const auto encoded = dsw::EliasFano::build(values, values.back());
    constexpr std::size_t kSelectIndices[] = {0U, 1U, 7U, 16U, 31U, 63U};
    const std::vector<std::uint64_t> contains_queries = {
        0U,
        values[0U],
        values[1U],
        values[7U],
        values[31U],
        values[63U],
        values[7U] + 1U,
        values[31U] + 1U,
    };
    const std::vector<std::uint64_t> predecessor_queries = {
        0U,
        values[0U],
        values[7U] + 3U,
        values[20U] - 1U,
        values[63U] + 5U,
    };

    emit_text("structure", "elias_fano");
    emit_int("count", encoded.size());
    emit_u64("universe_max", encoded.universe_max());
    emit_int("lower_bits", encoded.lower_bits());
    emit_int("memory_bytes", encoded.memory_bytes());

    for (const auto word : encoded.lower_words()) {
        emit_u64("lower_word", word);
    }
    for (const auto word : encoded.upper_words()) {
        emit_u64("upper_word", word);
    }
    for (const auto index : kSelectIndices) {
        emit_int("select_index", index);
        emit_u64("select_value", encoded.select(index));
    }
    for (const auto query : contains_queries) {
        emit_u64("contains_query", query);
        emit_bool("contains_result", encoded.contains(query));
    }
    for (const auto query : predecessor_queries) {
        emit_u64("predecessor_query", query);
        emit_optional_u64("predecessor_result", encoded.predecessor(query));
    }
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        print_usage();
        return EXIT_SUCCESS;
    }

    const std::string_view structure(argv[1]);
    if (structure == "blocked_bloom") {
        emit_blocked_bloom_parity();
        return EXIT_SUCCESS;
    }
    if (structure == "quotient_filter") {
        emit_quotient_filter_parity();
        return EXIT_SUCCESS;
    }
    if (structure == "elias_fano") {
        emit_elias_fano_parity();
        return EXIT_SUCCESS;
    }

    print_usage();
    return EXIT_FAILURE;
}
