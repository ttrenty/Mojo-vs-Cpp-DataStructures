"""Reference hashing helpers shared by setup scripts and tests."""

from __future__ import annotations

from hash_spec import load_hash_spec

MASK_64 = (1 << 64) - 1
SPEC = load_hash_spec()
SPLITMIX_INCREMENT = SPEC["splitmix64"]["increment"]
SPLITMIX_MUL1 = SPEC["splitmix64"]["mul1"]
SPLITMIX_MUL2 = SPEC["splitmix64"]["mul2"]

BLOOM_BLOCK_SEED = SPEC["domain_seeds"]["bloom_block_seed"]
BLOOM_STEP_SEED = SPEC["domain_seeds"]["bloom_step_seed"]
BLOOM_INDEX_SEED = SPEC["domain_seeds"]["bloom_index_seed"]
DATASET_KEY_SEED = SPEC["domain_seeds"]["dataset_key_seed"]
DATASET_NEGATIVE_SEED = SPEC["domain_seeds"]["dataset_negative_seed"]

RECOMMENDED_K_MINIMUM = SPEC["recommended_k_hashes"]["minimum"]
RECOMMENDED_K_NUMERATOR = SPEC["recommended_k_hashes"]["numerator"]
RECOMMENDED_K_ROUNDING = SPEC["recommended_k_hashes"]["rounding"]
RECOMMENDED_K_DENOMINATOR = SPEC["recommended_k_hashes"]["denominator"]

HIGH_BIT_MASK = SPEC["bit_layout"]["high_bit_mask"]
SIGNED_INT_MAX = SPEC["golden_cases"]["signed_int_max"]


def mask_u64(value: int) -> int:
    return value & MASK_64


def splitmix64(value: int, seed: int = 0) -> int:
    z = mask_u64(value + seed + SPLITMIX_INCREMENT)
    z = mask_u64((z ^ (z >> 30)) * SPLITMIX_MUL1)
    z = mask_u64((z ^ (z >> 27)) * SPLITMIX_MUL2)
    return mask_u64(z ^ (z >> 31))


def hash_uint64(key: int, seed: int = 0) -> int:
    return splitmix64(key, seed)


def recommended_k_hashes(bits_per_key: int) -> int:
    return max(
        RECOMMENDED_K_MINIMUM,
        (bits_per_key * RECOMMENDED_K_NUMERATOR + RECOMMENDED_K_ROUNDING) //
        RECOMMENDED_K_DENOMINATOR,
    )


def dataset_key(index: int) -> int:
    return hash_uint64(index, DATASET_KEY_SEED) & SIGNED_INT_MAX


def negative_query(index: int) -> int:
    return hash_uint64(index, DATASET_NEGATIVE_SEED) | HIGH_BIT_MASK
