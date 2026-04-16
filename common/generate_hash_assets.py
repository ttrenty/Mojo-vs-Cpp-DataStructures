from __future__ import annotations

import json
from pathlib import Path

from hash_reference import hash_uint64
from hash_spec import format_u64_hex, load_hash_spec

ROOT = Path(__file__).resolve().parents[1]
OUTPUT_ROOT = ROOT / "build" / "generated"
CPP_OUTPUT = OUTPUT_ROOT / "cpp" / "hash_constants.hpp"
MOJO_PACKAGE_DIR = OUTPUT_ROOT / "mojo" / "shared"
MOJO_INIT_OUTPUT = MOJO_PACKAGE_DIR / "__init__.mojo"
MOJO_OUTPUT = MOJO_PACKAGE_DIR / "hash_constants.mojo"
GOLDEN_OUTPUT = OUTPUT_ROOT / "hash_golden.json"


def build_cases(spec: dict[str, object]) -> list[dict[str, int]]:
    recipe = spec["golden_cases"]
    signed_int_max = int(recipe["signed_int_max"])
    cases: list[dict[str, int]] = []
    for index in range(int(recipe["count"])):
        key = hash_uint64(
            index * int(recipe["key_multiplier"]),
            int(recipe["key_seed"]),
        ) & signed_int_max
        seed = hash_uint64(
            index * int(recipe["seed_multiplier"]),
            int(recipe["seed_seed"]),
        ) & signed_int_max
        cases.append(
            {
                "index": index,
                "key": key,
                "seed": seed,
                "hash": hash_uint64(key, seed),
            }
        )
    return cases


def render_cpp(spec: dict[str, object]) -> str:
    splitmix = spec["splitmix64"]
    domain = spec["domain_seeds"]
    recommended = spec["recommended_k_hashes"]
    bit_layout = spec["bit_layout"]
    blocked_bloom = spec["blocked_bloom"]
    quotient_filter = spec["quotient_filter"]
    benchmark_labels = spec["benchmark_labels"]
    golden = spec["golden_cases"]
    lines = [
        "#pragma once",
        "",
        "#include <cstddef>",
        "#include <cstdint>",
        "",
        "namespace dsw {",
        "",
        f"inline constexpr std::uint64_t SPLITMIX_INCREMENT = {format_u64_hex(int(splitmix['increment']))}ULL;",
        f"inline constexpr std::uint64_t SPLITMIX_MUL1 = {format_u64_hex(int(splitmix['mul1']))}ULL;",
        f"inline constexpr std::uint64_t SPLITMIX_MUL2 = {format_u64_hex(int(splitmix['mul2']))}ULL;",
        "",
        f"inline constexpr std::uint64_t BLOOM_BLOCK_SEED = {format_u64_hex(int(domain['bloom_block_seed']))}ULL;",
        f"inline constexpr std::uint64_t BLOOM_STEP_SEED = {format_u64_hex(int(domain['bloom_step_seed']))}ULL;",
        f"inline constexpr std::uint64_t BLOOM_INDEX_SEED = {format_u64_hex(int(domain['bloom_index_seed']))}ULL;",
        f"inline constexpr std::uint64_t DATASET_KEY_SEED = {format_u64_hex(int(domain['dataset_key_seed']))}ULL;",
        f"inline constexpr std::uint64_t DATASET_NEGATIVE_SEED = {format_u64_hex(int(domain['dataset_negative_seed']))}ULL;",
        "",
        f"inline constexpr std::size_t RECOMMENDED_K_MINIMUM = {int(recommended['minimum'])}U;",
        f"inline constexpr std::size_t RECOMMENDED_K_NUMERATOR = {int(recommended['numerator'])}U;",
        f"inline constexpr std::size_t RECOMMENDED_K_ROUNDING = {int(recommended['rounding'])}U;",
        f"inline constexpr std::size_t RECOMMENDED_K_DENOMINATOR = {int(recommended['denominator'])}U;",
        "",
        f"inline constexpr std::size_t WORD_BITS = {int(bit_layout['word_bits'])}U;",
        f"inline constexpr std::size_t WORD_SHIFT = {int(bit_layout['word_shift'])}U;",
        f"inline constexpr std::size_t WORD_MASK = {int(bit_layout['word_mask'])}U;",
        f"inline constexpr std::uint64_t HIGH_BIT_MASK = {format_u64_hex(int(bit_layout['high_bit_mask']))}ULL;",
        "",
        f"inline constexpr std::size_t BLOCKED_BLOOM_BLOCK_BITS = {int(blocked_bloom['block_bits'])}U;",
        f"inline constexpr std::size_t BLOCKED_BLOOM_WORDS_PER_BLOCK = {int(blocked_bloom['words_per_block'])}U;",
        "",
        f"inline constexpr std::size_t QUOTIENT_FILTER_CLUSTER_RESERVE = {int(quotient_filter['initial_cluster_reserve'])}U;",
        "",
        f"inline constexpr std::size_t DENSE_UNIVERSE_FACTOR_MAX = {int(benchmark_labels['dense_universe_factor_max'])}U;",
        f"inline constexpr std::size_t MEDIUM_UNIVERSE_FACTOR_MAX = {int(benchmark_labels['medium_universe_factor_max'])}U;",
        "",
        f"inline constexpr std::size_t GOLDEN_CASE_COUNT = {int(golden['count'])}U;",
        f"inline constexpr std::uint64_t GOLDEN_KEY_MULTIPLIER = {format_u64_hex(int(golden['key_multiplier']))}ULL;",
        f"inline constexpr std::uint64_t GOLDEN_KEY_SEED = {format_u64_hex(int(golden['key_seed']))}ULL;",
        f"inline constexpr std::uint64_t GOLDEN_SEED_MULTIPLIER = {format_u64_hex(int(golden['seed_multiplier']))}ULL;",
        f"inline constexpr std::uint64_t GOLDEN_SEED_SEED = {format_u64_hex(int(golden['seed_seed']))}ULL;",
        f"inline constexpr std::uint64_t SIGNED_INT_MAX = {format_u64_hex(int(golden['signed_int_max']))}ULL;",
        "",
        "inline constexpr std::uint64_t kSplitMixIncrement = SPLITMIX_INCREMENT;",
        "inline constexpr std::uint64_t kSplitMixMul1 = SPLITMIX_MUL1;",
        "inline constexpr std::uint64_t kSplitMixMul2 = SPLITMIX_MUL2;",
        "inline constexpr std::uint64_t kBloomBlockSeed = BLOOM_BLOCK_SEED;",
        "inline constexpr std::uint64_t kBloomStepSeed = BLOOM_STEP_SEED;",
        "inline constexpr std::uint64_t kBloomIndexSeed = BLOOM_INDEX_SEED;",
        "inline constexpr std::uint64_t kDatasetKeySeed = DATASET_KEY_SEED;",
        "inline constexpr std::uint64_t kDatasetNegativeSeed = DATASET_NEGATIVE_SEED;",
        "",
        "}  // namespace dsw",
    ]
    return "\n".join(lines) + "\n"


def render_mojo(spec: dict[str, object]) -> str:
    splitmix = spec["splitmix64"]
    domain = spec["domain_seeds"]
    recommended = spec["recommended_k_hashes"]
    bit_layout = spec["bit_layout"]
    blocked_bloom = spec["blocked_bloom"]
    quotient_filter = spec["quotient_filter"]
    benchmark_labels = spec["benchmark_labels"]
    golden = spec["golden_cases"]
    lines = [
        f"comptime SPLITMIX_INCREMENT = {format_u64_hex(int(splitmix['increment']))}",
        f"comptime SPLITMIX_MUL1 = {format_u64_hex(int(splitmix['mul1']))}",
        f"comptime SPLITMIX_MUL2 = {format_u64_hex(int(splitmix['mul2']))}",
        "",
        f"comptime BLOOM_BLOCK_SEED = {format_u64_hex(int(domain['bloom_block_seed']))}",
        f"comptime BLOOM_STEP_SEED = {format_u64_hex(int(domain['bloom_step_seed']))}",
        f"comptime BLOOM_INDEX_SEED = {format_u64_hex(int(domain['bloom_index_seed']))}",
        f"comptime DATASET_KEY_SEED = {format_u64_hex(int(domain['dataset_key_seed']))}",
        f"comptime DATASET_NEGATIVE_SEED = {format_u64_hex(int(domain['dataset_negative_seed']))}",
        "",
        f"comptime RECOMMENDED_K_MINIMUM = {int(recommended['minimum'])}",
        f"comptime RECOMMENDED_K_NUMERATOR = {int(recommended['numerator'])}",
        f"comptime RECOMMENDED_K_ROUNDING = {int(recommended['rounding'])}",
        f"comptime RECOMMENDED_K_DENOMINATOR = {int(recommended['denominator'])}",
        "",
        f"comptime WORD_BITS = {int(bit_layout['word_bits'])}",
        f"comptime WORD_SHIFT = {int(bit_layout['word_shift'])}",
        f"comptime WORD_MASK = {int(bit_layout['word_mask'])}",
        f"comptime HIGH_BIT_MASK = {format_u64_hex(int(bit_layout['high_bit_mask']))}",
        "",
        f"comptime BLOCKED_BLOOM_BLOCK_BITS = {int(blocked_bloom['block_bits'])}",
        f"comptime BLOCKED_BLOOM_WORDS_PER_BLOCK = {int(blocked_bloom['words_per_block'])}",
        "",
        f"comptime QUOTIENT_FILTER_CLUSTER_RESERVE = {int(quotient_filter['initial_cluster_reserve'])}",
        "",
        f"comptime DENSE_UNIVERSE_FACTOR_MAX = {int(benchmark_labels['dense_universe_factor_max'])}",
        f"comptime MEDIUM_UNIVERSE_FACTOR_MAX = {int(benchmark_labels['medium_universe_factor_max'])}",
        "",
        f"comptime GOLDEN_CASE_COUNT = {int(golden['count'])}",
        f"comptime GOLDEN_KEY_MULTIPLIER = {format_u64_hex(int(golden['key_multiplier']))}",
        f"comptime GOLDEN_KEY_SEED = {format_u64_hex(int(golden['key_seed']))}",
        f"comptime GOLDEN_SEED_MULTIPLIER = {format_u64_hex(int(golden['seed_multiplier']))}",
        f"comptime GOLDEN_SEED_SEED = {format_u64_hex(int(golden['seed_seed']))}",
        f"comptime SIGNED_INT_MAX = {format_u64_hex(int(golden['signed_int_max']))}",
        "",
    ]
    return "\n".join(lines)


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def main() -> None:
    spec = load_hash_spec()
    cases = build_cases(spec)
    payload = {
        "description": "SplitMix64 golden file for cross-language conformance.",
        "spec_path": "common/hash_spec.json",
        "cases": cases,
    }

    write_text(CPP_OUTPUT, render_cpp(spec))
    write_text(MOJO_INIT_OUTPUT, "")
    write_text(MOJO_OUTPUT, render_mojo(spec))
    write_text(GOLDEN_OUTPUT, json.dumps(payload, indent=2) + "\n")

    print(f"Wrote generated hash assets under {OUTPUT_ROOT}")


if __name__ == "__main__":
    main()
