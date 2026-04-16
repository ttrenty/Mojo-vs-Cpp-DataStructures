from db_structures.hash import DATASET_KEY_SEED, dataset_key, hash_uint64, negative_query
from shared.hash_constants import DENSE_UNIVERSE_FACTOR_MAX, HIGH_BIT_MASK, MEDIUM_UNIVERSE_FACTOR_MAX


def make_deterministic_keys(
    count: Int, seed: UInt64, set_high_bit: Bool
) -> List[UInt64]:
    var values = List[UInt64](length=count, fill=0)
    var high_bit = UInt64(HIGH_BIT_MASK)
    for index in range(count):
        var value = hash_uint64(UInt64(index), seed)
        if set_high_bit:
            value |= high_bit
        else:
            value &= ~high_bit
        values[index] = value
    return values^


def default_positive_keys(count: Int) -> List[UInt64]:
    var values = List[UInt64](length=count, fill=0)
    for index in range(count):
        values[index] = dataset_key(UInt64(index))
    return values^


def default_negative_queries(count: Int) -> List[UInt64]:
    var values = List[UInt64](length=count, fill=0)
    for index in range(count):
        values[index] = negative_query(UInt64(index))
    return values^


def make_mixed_queries(
    read positives: List[UInt64], read negatives: List[UInt64]
) -> List[UInt64]:
    var count = len(positives)
    if len(negatives) < count:
        count = len(negatives)

    var mixed = List[UInt64](length=count * 2, fill=0)
    for index in range(count):
        mixed[index * 2] = positives[index]
        mixed[index * 2 + 1] = negatives[index]
    return mixed^


def ns_per_op(duration_ns: UInt, ops: Int) -> Float64:
    return Float64(duration_ns) / Float64(ops)


def make_sorted_unique_values(count: Int, universe_factor: Int) -> List[UInt64]:
    var stride = universe_factor
    if stride < 1:
        stride = 1

    var values = List[UInt64](length=count, fill=0)
    for index in range(count):
        var jitter = hash_uint64(UInt64(index), UInt64(DATASET_KEY_SEED)) % UInt64(stride)
        values[index] = UInt64(index * stride) + jitter
    return values^


def density_label(universe_factor: Int) -> StaticString:
    if universe_factor <= DENSE_UNIVERSE_FACTOR_MAX:
        return "dense"
    if universe_factor <= MEDIUM_UNIVERSE_FACTOR_MAX:
        return "medium"
    return "sparse"
