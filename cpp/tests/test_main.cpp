#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <string_view>
#include <unordered_set>

#include "benchmark_utils.hpp"
#include "blocked_bloom.hpp"
#include "elias_fano.hpp"
#include "hash.hpp"
#include "quotient_filter.hpp"

namespace {

void expect(bool condition, std::string_view message) {
    if (!condition) {
        throw std::runtime_error(std::string(message));
    }
}

void test_splitmix64_regressions() {
    expect(dsw::hash_uint64(0U, 0U) == 16294208416658607535ULL, "hash(0, 0)");
    expect(dsw::hash_uint64(1U, 0U) == 10451216379200822465ULL, "hash(1, 0)");
    expect(dsw::hash_uint64(42U, 0U) == 13679457532755275413ULL, "hash(42, 0)");
    expect(dsw::hash_uint64(42U, 17U) == 10849667979899222076ULL, "hash(42, 17)");
    expect(dsw::hash_uint64(65535U, 7U) == 10425279917709650475ULL, "hash(65535, 7)");
    expect(dsw::hash_uint64(4294967295ULL, 11U) == 1385141570936672053ULL, "hash(max32, 11)");
    expect(dsw::hash_uint64(281474976723001ULL, 19U) == 17163411026733143224ULL, "hash(48bit, 19)");
    expect(dsw::hash_uint64(9223372036854775807ULL, 23U) == 9057062024509780834ULL, "hash(max63, 23)");
    expect(dsw::hash_uint64(18446744073709551614ULL, 29U) == 10902710238276814474ULL, "hash(max64-1, 29)");
    expect(dsw::hash_uint64(3735928559ULL, 1311768467294899695ULL) == 12169283692704944148ULL, "hash(0xDEADBEEF, pi)");
    expect(dsw::hash_uint64(81985529216486895ULL, 1229782938247303441ULL) == 13982401800937507175ULL, "hash(pattern1)");
    expect(dsw::hash_uint64(18364758544493064720ULL, 2459565876494606882ULL) == 3390646966426288709ULL, "hash(pattern2)");
    expect(dsw::hash_uint64(12297829382473034410ULL, 3689348814741910323ULL) == 13328325046192817247ULL, "hash(pattern3)");
    expect(dsw::hash_uint64(6148914691236517205ULL, 4919131752989213764ULL) == 10561564521192673552ULL, "hash(pattern4)");
}

void test_hash_helper_regressions() {
    expect(dsw::recommended_k_hashes(0U) == 1U, "recommended_k_hashes(0)");
    expect(dsw::recommended_k_hashes(1U) == 1U, "recommended_k_hashes(1)");
    expect(dsw::recommended_k_hashes(10U) == 7U, "recommended_k_hashes(10)");
    expect((dsw::dataset_key(0U) >> 63U) == 0U, "dataset_key must keep high bit clear");
    expect((dsw::negative_query(0U) >> 63U) == 1U, "negative_query must set high bit");
}

void test_blocked_bloom_no_false_negatives() {
    const auto keys = dsw::default_positive_keys(20'000U);
    const auto negatives = dsw::default_negative_queries(20'000U);
    auto filter = dsw::BlockedBloomFilter::build(keys, 10U, dsw::recommended_k_hashes(10U));

    for (const auto key : keys) {
        expect(filter.contains(key), "inserted key must be present");
    }

    const double fpr = dsw::empirical_false_positive_rate(filter, negatives);
    expect(fpr >= 0.0 && fpr < 0.2, "unexpected blocked bloom FPR");
}

void test_blocked_bloom_fpr_improves_with_more_bits() {
    const auto keys = dsw::default_positive_keys(25'000U);
    const auto negatives = dsw::default_negative_queries(25'000U);
    const auto low = dsw::BlockedBloomFilter::build(keys, 8U, dsw::recommended_k_hashes(8U));
    const auto high = dsw::BlockedBloomFilter::build(keys, 14U, dsw::recommended_k_hashes(14U));

    const double low_fpr = dsw::empirical_false_positive_rate(low, negatives);
    const double high_fpr = dsw::empirical_false_positive_rate(high, negatives);

    expect(low.memory_bytes() < high.memory_bytes(), "higher bits/key should use more memory");
    expect(low_fpr > high_fpr, "FPR should improve when bits/key increases");
}

void test_blocked_bloom_block_alignment() {
    dsw::BlockedBloomFilter filter(1000U, 10U, dsw::recommended_k_hashes(10U));
    expect(filter.memory_bytes() % 64U == 0U, "blocked bloom storage must stay cache-line aligned");
    expect(sizeof(dsw::BlockedBloomFilter::Block) == 64U, "Block struct must be exactly 64 bytes");
    expect(alignof(dsw::BlockedBloomFilter::Block) == 64U, "Block struct must be 64-byte aligned");
}

void test_quotient_filter_insert_contains_erase() {
    auto filter = dsw::QuotientFilter::create(12U, 10U);
    const auto keys = dsw::default_positive_keys(1500U);

    for (std::size_t index = 0; index < 1000U; ++index) {
        expect(filter.insert(keys[index]), "insert should succeed");
    }
    for (std::size_t index = 0; index < 1000U; ++index) {
        expect(filter.contains(keys[index]), "inserted key must be present in QF");
    }
    for (std::size_t index = 0; index < 250U; ++index) {
        expect(
            filter.erase(keys[index]),
            std::string("erase should succeed at index ") + std::to_string(index)
        );
        expect(
            !filter.contains(keys[index]),
            std::string("erased key should disappear at index ") + std::to_string(index)
        );
        for (std::size_t survivor = index + 1U; survivor < 1000U; ++survivor) {
            expect(
                filter.contains(keys[survivor]),
                std::string("remaining key vanished after erase step ") + std::to_string(index) +
                    " at survivor " + std::to_string(survivor)
            );
        }
    }

    expect(filter.load_factor() > 0.0, "load factor should be positive");
    expect(filter.memory_bytes() == filter.capacity() * sizeof(std::uint32_t), "packed slot bytes");
}

void test_quotient_filter_mixed_workload_matches_reference_set() {
    auto filter = dsw::QuotientFilter::create(13U, 12U);
    std::unordered_set<std::uint64_t> reference;
    const auto positives = dsw::default_positive_keys(4000U);
    const auto negatives = dsw::default_negative_queries(4000U);

    for (std::size_t index = 0; index < 1500U; ++index) {
        expect(filter.insert(positives[index]), "initial insert should succeed");
        reference.insert(positives[index]);
    }

    for (std::size_t step = 0; step < 2000U; ++step) {
        const std::uint64_t insert_key = positives[(step + 1500U) % positives.size()];
        const std::uint64_t erase_key = positives[step % positives.size()];
        const std::uint64_t query_key =
            step % 2U == 0U ? positives[step % positives.size()] : negatives[step % negatives.size()];

        if (step % 10U == 0U) {
            const bool erased = filter.erase(erase_key);
            const bool expected_erased = reference.erase(erase_key) > 0U;
            expect(erased == expected_erased, "erase result should match reference set");
        } else if (step % 2U == 0U) {
            const bool inserted = filter.insert(insert_key);
            const bool expected_inserted = reference.insert(insert_key).second;
            expect(inserted == expected_inserted, "insert result should match reference set");
        }

        const bool contains = filter.contains(query_key);
        const bool expected_contains = reference.contains(query_key);
        if (expected_contains) {
            expect(contains, "QF must not report false negatives");
        }
    }

    expect(filter.average_cluster_length() >= 1.0, "cluster stats should remain sane");
}

void test_elias_fano_select_contains_and_predecessor() {
    const auto values = dsw::make_sorted_unique_values(4000U, 64U);
    const auto encoded = dsw::EliasFano::build(values, values.back());

    for (std::size_t index = 0; index < values.size(); index += 257U) {
        expect(encoded.select(index) == values[index], "select should recover stored value");
        expect(encoded.contains(values[index]), "contains should find stored values");
    }

    const std::uint64_t mid_value = values[1700U] + 3U;
    const auto predecessor = encoded.predecessor(mid_value);
    expect(predecessor.has_value(), "predecessor should exist");
    expect(predecessor.value() == values[1700U], "predecessor should match sorted baseline");
    expect(encoded.memory_bytes() < values.size() * sizeof(std::uint64_t), "compressed payload");
}

int run_test_suite() {
    test_splitmix64_regressions();
    test_hash_helper_regressions();
    test_blocked_bloom_no_false_negatives();
    test_blocked_bloom_fpr_improves_with_more_bits();
    test_blocked_bloom_block_alignment();
    test_quotient_filter_insert_contains_erase();
    test_quotient_filter_mixed_workload_matches_reference_set();
    test_elias_fano_select_contains_and_predecessor();
    std::cout << "All C++ tests passed.\n";
    return EXIT_SUCCESS;
}

}  // namespace

int main(int argc, char** argv) {
    (void)argc;
    (void)argv;
    try {
        return run_test_suite();
    } catch (const std::exception& error) {
        std::cerr << "C++ test failure: " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
