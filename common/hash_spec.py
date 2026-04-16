from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
SPEC_PATH = ROOT / "hash_spec.json"


def parse_u64_hex(raw: str) -> int:
    return int(raw, 16)


def format_u64_hex(value: int) -> str:
    return f"0x{value:016X}"


@lru_cache(maxsize=1)
def load_hash_spec() -> dict[str, Any]:
    payload = json.loads(SPEC_PATH.read_text(encoding="utf-8"))
    splitmix = payload["splitmix64"]
    domain_seeds = payload["domain_seeds"]
    recommended = payload["recommended_k_hashes"]
    bit_layout = payload["bit_layout"]
    blocked_bloom = payload["blocked_bloom"]
    quotient_filter = payload["quotient_filter"]
    benchmark_labels = payload["benchmark_labels"]
    golden = payload["golden_cases"]

    return {
        "splitmix64": {
            "increment": parse_u64_hex(splitmix["increment"]),
            "mul1": parse_u64_hex(splitmix["mul1"]),
            "mul2": parse_u64_hex(splitmix["mul2"]),
        },
        "domain_seeds": {
            "bloom_block_seed": parse_u64_hex(domain_seeds["bloom_block_seed"]),
            "bloom_step_seed": parse_u64_hex(domain_seeds["bloom_step_seed"]),
            "bloom_index_seed": parse_u64_hex(domain_seeds["bloom_index_seed"]),
            "dataset_key_seed": parse_u64_hex(domain_seeds["dataset_key_seed"]),
            "dataset_negative_seed": parse_u64_hex(domain_seeds["dataset_negative_seed"]),
        },
        "recommended_k_hashes": {
            "minimum": int(recommended["minimum"]),
            "numerator": int(recommended["numerator"]),
            "rounding": int(recommended["rounding"]),
            "denominator": int(recommended["denominator"]),
        },
        "bit_layout": {
            "word_bits": int(bit_layout["word_bits"]),
            "word_shift": int(bit_layout["word_shift"]),
            "word_mask": int(bit_layout["word_mask"]),
            "high_bit_mask": parse_u64_hex(bit_layout["high_bit_mask"]),
        },
        "blocked_bloom": {
            "block_bits": int(blocked_bloom["block_bits"]),
            "words_per_block": int(blocked_bloom["words_per_block"]),
        },
        "quotient_filter": {
            "initial_cluster_reserve": int(quotient_filter["initial_cluster_reserve"]),
        },
        "benchmark_labels": {
            "dense_universe_factor_max": int(benchmark_labels["dense_universe_factor_max"]),
            "medium_universe_factor_max": int(benchmark_labels["medium_universe_factor_max"]),
        },
        "golden_cases": {
            "count": int(golden["count"]),
            "key_multiplier": parse_u64_hex(golden["key_multiplier"]),
            "key_seed": parse_u64_hex(golden["key_seed"]),
            "seed_multiplier": parse_u64_hex(golden["seed_multiplier"]),
            "seed_seed": parse_u64_hex(golden["seed_seed"]),
            "signed_int_max": parse_u64_hex(golden["signed_int_max"]),
        },
    }
