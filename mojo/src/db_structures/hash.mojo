from shared.hash_constants import (
    BLOOM_BLOCK_SEED,
    BLOOM_INDEX_SEED,
    BLOOM_STEP_SEED,
    DATASET_KEY_SEED,
    DATASET_NEGATIVE_SEED,
    HIGH_BIT_MASK,
    RECOMMENDED_K_DENOMINATOR,
    RECOMMENDED_K_MINIMUM,
    RECOMMENDED_K_NUMERATOR,
    RECOMMENDED_K_ROUNDING,
    SIGNED_INT_MAX,
    SPLITMIX_INCREMENT,
    SPLITMIX_MUL1,
    SPLITMIX_MUL2,
)


@always_inline
def splitmix64(value: UInt64, seed: UInt64 = 0) -> UInt64:
    var z = value + seed + UInt64(SPLITMIX_INCREMENT)
    z = (z ^ (z >> 30)) * UInt64(SPLITMIX_MUL1)
    z = (z ^ (z >> 27)) * UInt64(SPLITMIX_MUL2)
    return z ^ (z >> 31)


@always_inline
def hash_uint64(key: UInt64, seed: UInt64 = 0) -> UInt64:
    return splitmix64(key, seed)


@always_inline
def recommended_k_hashes(bits_per_key: Int) -> Int:
    # For these non-negative sizing terms, Mojo's floor division matches the
    # C++ helper's unsigned integer division exactly.
    var estimate = (bits_per_key * RECOMMENDED_K_NUMERATOR + RECOMMENDED_K_ROUNDING) // RECOMMENDED_K_DENOMINATOR
    if estimate < RECOMMENDED_K_MINIMUM:
        return RECOMMENDED_K_MINIMUM
    return estimate


@always_inline
def dataset_key(index: UInt64) -> UInt64:
    return hash_uint64(index, UInt64(DATASET_KEY_SEED)) & UInt64(SIGNED_INT_MAX)


@always_inline
def negative_query(index: UInt64) -> UInt64:
    return hash_uint64(index, UInt64(DATASET_NEGATIVE_SEED)) | UInt64(HIGH_BIT_MASK)
