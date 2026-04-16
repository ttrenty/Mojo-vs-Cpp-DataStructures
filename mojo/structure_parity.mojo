from std.sys import argv

from db_structures.benchmark_utils import (
    default_negative_queries,
    default_positive_keys,
    make_sorted_unique_values,
)
from db_structures.blocked_bloom import BlockedBloomFilter
from db_structures.elias_fano import EliasFano
from db_structures.hash import recommended_k_hashes
from db_structures.quotient_filter import create_quotient_filter


def emit_text(name: StaticString, value: StaticString):
    print(String.write(t"{name}={value}"))


def emit_int(name: StaticString, value: Int):
    print(String.write(t"{name}={value}"))


def emit_u64(name: StaticString, value: UInt64):
    print(String.write(t"{name}={value}"))


def emit_u32(name: StaticString, value: UInt32):
    print(String.write(t"{name}={value}"))


def emit_bool(name: StaticString, value: Bool):
    print(String.write(t"{name}={1 if value else 0}"))


def emit_optional_u64(name: StaticString, value: Optional[UInt64]):
    if value == None:
        emit_text(name, "none")
        return
    emit_u64(name, value.value())


def print_usage():
    print("usage: mojo_structure_parity <blocked_bloom|quotient_filter|elias_fano>")


def emit_blocked_bloom_parity() raises:
    var bits_per_key = 10
    var k_hashes = recommended_k_hashes(bits_per_key)
    var positives = default_positive_keys(24)
    var negatives = default_negative_queries(12)
    var filter = BlockedBloomFilter.build(positives, bits_per_key, k_hashes)

    emit_text("structure", "blocked_bloom")
    emit_int("bits_per_key", bits_per_key)
    emit_int("k_hashes", k_hashes)
    emit_int("memory_bytes", filter.memory_bytes())

    for word in filter.debug_block_words():
        emit_u64("block_word", word)
    for key in positives:
        emit_u64("positive_key", key)
        emit_bool("positive_contains", filter.contains(key))
    for key in negatives:
        emit_u64("negative_key", key)
        emit_bool("negative_contains", filter.contains(key))


def emit_quotient_filter_parity() raises:
    var filter = create_quotient_filter(6, 8)
    var positives = default_positive_keys(32)
    var negatives = default_negative_queries(8)
    var erase_indices = List[Int]()
    erase_indices.append(3)
    erase_indices.append(7)
    erase_indices.append(11)
    erase_indices.append(15)

    emit_text("structure", "quotient_filter")
    emit_int("capacity_pow2", 6)
    emit_int("remainder_bits", 8)

    for index in range(20):
        emit_bool("insert_result", filter.insert(positives[index]))
    for erase_index in erase_indices:
        emit_u64("erase_key", positives[erase_index])
        emit_bool("erase_result", filter.erase(positives[erase_index]))
    for index in range(20, 28):
        emit_bool("insert_result", filter.insert(positives[index]))

    var instrumentation = filter.instrumentation()
    emit_int("count", filter.count)
    emit_int("capacity", filter.capacity)
    emit_int("insert_count", instrumentation.insert_count)
    emit_int("total_probe_distance", instrumentation.total_probe_distance)
    emit_int("max_probe_distance", instrumentation.max_probe_distance)
    emit_int("total_cluster_length", instrumentation.total_cluster_length)
    emit_int("max_cluster_length", instrumentation.max_cluster_length)

    for slot in filter.raw_slots():
        emit_u32("slot", slot)
    for key in positives:
        emit_u64("positive_key", key)
        emit_bool("positive_contains", filter.contains(key))
    for key in negatives:
        emit_u64("negative_key", key)
        emit_bool("negative_contains", filter.contains(key))


def emit_elias_fano_parity() raises:
    var values = make_sorted_unique_values(64, 64)
    var encoded = EliasFano.build(values, values[len(values) - 1])
    var select_indices = List[Int]()
    select_indices.append(0)
    select_indices.append(1)
    select_indices.append(7)
    select_indices.append(16)
    select_indices.append(31)
    select_indices.append(63)
    var contains_queries = List[UInt64]()
    contains_queries.append(UInt64(0))
    contains_queries.append(values[0])
    contains_queries.append(values[1])
    contains_queries.append(values[7])
    contains_queries.append(values[31])
    contains_queries.append(values[63])
    contains_queries.append(values[7] + UInt64(1))
    contains_queries.append(values[31] + UInt64(1))
    var predecessor_queries = List[UInt64]()
    predecessor_queries.append(UInt64(0))
    predecessor_queries.append(values[0])
    predecessor_queries.append(values[7] + UInt64(3))
    predecessor_queries.append(values[20] - UInt64(1))
    predecessor_queries.append(values[63] + UInt64(5))

    emit_text("structure", "elias_fano")
    emit_int("count", encoded.count)
    emit_u64("universe_max", encoded.universe_max)
    emit_int("lower_bits", encoded.lower_bits)
    emit_int("memory_bytes", encoded.memory_bytes())

    for word in encoded.lower_words():
        emit_u64("lower_word", word)
    for word in encoded.upper_words():
        emit_u64("upper_word", word)
    for index in select_indices:
        emit_int("select_index", index)
        emit_u64("select_value", encoded.select(index))
    for query in contains_queries:
        emit_u64("contains_query", query)
        emit_bool("contains_result", encoded.contains(query))
    for query in predecessor_queries:
        emit_u64("predecessor_query", query)
        emit_optional_u64("predecessor_result", encoded.predecessor(query))


def main() raises:
    var args = argv()
    if len(args) < 2:
        print_usage()
        return

    if args[1] == "blocked_bloom":
        emit_blocked_bloom_parity()
        return
    if args[1] == "quotient_filter":
        emit_quotient_filter_parity()
        return
    if args[1] == "elias_fano":
        emit_elias_fano_parity()
        return

    print_usage()
    raise Error("unknown parity structure")
