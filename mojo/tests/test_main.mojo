from std.testing import TestSuite, assert_equal, assert_false, assert_true

from db_structures.benchmark_utils import (
    default_negative_queries,
    default_positive_keys,
    make_sorted_unique_values,
)
from db_structures.blocked_bloom import BlockedBloomFilter, empirical_false_positive_rate
from db_structures.elias_fano import EliasFano
from db_structures.hash import dataset_key, hash_uint64, negative_query, recommended_k_hashes, splitmix64
from db_structures.quotient_filter import create_quotient_filter


def test_hash_regressions() raises:
    assert_equal(hash_uint64(0, 0), UInt64(16294208416658607535))
    assert_equal(hash_uint64(1, 0), UInt64(10451216379200822465))
    assert_equal(hash_uint64(42, 0), UInt64(13679457532755275413))
    assert_equal(hash_uint64(42, 17), UInt64(10849667979899222076))
    assert_equal(hash_uint64(65535, 7), UInt64(10425279917709650475))
    assert_equal(hash_uint64(UInt64(4294967295), UInt64(11)), UInt64(1385141570936672053))
    assert_equal(hash_uint64(UInt64(281474976723001), UInt64(19)), UInt64(17163411026733143224))
    assert_equal(hash_uint64(UInt64(9223372036854775807), UInt64(23)), UInt64(9057062024509780834))
    assert_equal(hash_uint64(UInt64(18446744073709551614), UInt64(29)), UInt64(10902710238276814474))
    assert_equal(
        hash_uint64(UInt64(3735928559), UInt64(1311768467294899695)),
        UInt64(12169283692704944148),
    )
    assert_equal(
        hash_uint64(UInt64(81985529216486895), UInt64(1229782938247303441)),
        UInt64(13982401800937507175),
    )
    assert_equal(
        hash_uint64(UInt64(18364758544493064720), UInt64(2459565876494606882)),
        UInt64(3390646966426288709),
    )
    assert_equal(
        hash_uint64(UInt64(12297829382473034410), UInt64(3689348814741910323)),
        UInt64(13328325046192817247),
    )
    assert_equal(
        hash_uint64(UInt64(6148914691236517205), UInt64(4919131752989213764)),
        UInt64(10561564521192673552),
    )


def test_hash_helper_regressions() raises:
    assert_equal(recommended_k_hashes(0), 1)
    assert_equal(recommended_k_hashes(1), 1)
    assert_equal(recommended_k_hashes(10), 7)
    assert_equal(dataset_key(0) >> 63, UInt64(0))
    assert_equal(negative_query(0) >> 63, UInt64(1))


def test_blocked_bloom_no_false_negatives() raises:
    var keys = default_positive_keys(20_000)
    var negatives = default_negative_queries(20_000)
    var filter = BlockedBloomFilter.build(keys, 10, recommended_k_hashes(10))

    for key in keys:
        assert_true(filter.contains(key))

    var fpr = empirical_false_positive_rate(filter, negatives)
    assert_true(fpr >= 0.0)
    assert_true(fpr < 0.2)


def test_blocked_bloom_fpr_improves_with_more_bits() raises:
    var keys = default_positive_keys(25_000)
    var negatives = default_negative_queries(25_000)
    var low = BlockedBloomFilter.build(keys, 8, recommended_k_hashes(8))
    var high = BlockedBloomFilter.build(keys, 14, recommended_k_hashes(14))

    assert_true(low.memory_bytes() < high.memory_bytes())
    assert_true(empirical_false_positive_rate(low, negatives) > empirical_false_positive_rate(high, negatives))


def test_blocked_bloom_does_not_report_clearly_negative_keys() raises:
    var keys = default_positive_keys(8_000)
    var negatives = default_negative_queries(8_000)
    var filter = BlockedBloomFilter.build(keys, 12, recommended_k_hashes(12))
    assert_false(filter.contains(UInt64(0xFFFF_FFFF_FFFF_FFFF)))
    assert_true(empirical_false_positive_rate(filter, negatives) < 0.1)


def test_quotient_filter_insert_contains_and_erase() raises:
    var filter = create_quotient_filter(12, 10)
    var keys = default_positive_keys(1_500)

    for index in range(1_000):
        assert_true(filter.insert(keys[index]))
    for index in range(1_000):
        assert_true(filter.contains(keys[index]))
    for index in range(250):
        assert_true(filter.erase(keys[index]))
        assert_false(filter.contains(keys[index]))
        for survivor in range(index + 1, 1_000):
            assert_true(filter.contains(keys[survivor]))

    assert_true(filter.load_factor() > 0.0)
    assert_true(filter.memory_bytes() > 0)


def test_quotient_filter_mixed_workload_stays_consistent() raises:
    var filter = create_quotient_filter(13, 12)
    var positives = default_positive_keys(4_000)
    var negatives = default_negative_queries(4_000)
    var present = List[Bool](length=4_000, fill=False)

    for index in range(1_500):
        assert_true(filter.insert(positives[index]))
        present[index] = True

    for step in range(2_000):
        var insert_index = (step + 1_500) % 4_000
        var erase_index = step % 4_000

        if step % 10 == 0:
            var erased = filter.erase(positives[erase_index])
            assert_equal(erased, present[erase_index])
            present[erase_index] = False
        elif step % 2 == 0:
            var inserted = filter.insert(positives[insert_index])
            assert_equal(inserted, not present[insert_index])
            present[insert_index] = True

        var query_index = step % 4_000
        if step % 2 == 0 and present[query_index]:
            assert_true(filter.contains(positives[query_index]))
        else:
            if present[query_index]:
                assert_true(filter.contains(positives[query_index]))
            else:
                _ = filter.contains(negatives[query_index])

    assert_true(filter.average_cluster_length() >= 1.0)


def test_elias_fano_select_contains_and_predecessor() raises:
    var values = make_sorted_unique_values(4_000, 64)
    var encoded = EliasFano.build(values, values[len(values) - 1])

    var indices = List[Int]()
    indices.append(0)
    indices.append(257)
    indices.append(1024)
    indices.append(2048)
    indices.append(3999)
    for index in indices:
        assert_equal(encoded.select(index), values[index])
        assert_true(encoded.contains(values[index]))

    var predecessor = encoded.predecessor(values[1700] + UInt64(3))
    assert_true(predecessor != None)
    assert_equal(predecessor.value(), values[1700])
    assert_true(encoded.memory_bytes() < len(values) * 8)


def main() raises:
    TestSuite.discover_tests[__functions_in_module()]().run()
