from __future__ import annotations

import csv
import re
from pathlib import Path

from compare_implementations import parse_cpp_symbols, parse_mojo_symbols

ROOT = Path(__file__).resolve().parent.parent
OUTPUT = ROOT / "results" / "processed" / "code_metrics.csv"

FILE_GROUPS = [
    (
        "cpp",
        "blocked_bloom",
        [ROOT / "cpp" / "include" / "blocked_bloom.hpp"],
    ),
    (
        "cpp",
        "quotient_filter",
        [ROOT / "cpp" / "include" / "quotient_filter.hpp"],
    ),
    (
        "cpp",
        "elias_fano",
        [ROOT / "cpp" / "include" / "elias_fano.hpp"],
    ),
    (
        "mojo",
        "blocked_bloom",
        [ROOT / "mojo" / "src" / "db_structures" / "blocked_bloom.mojo"],
    ),
    (
        "mojo",
        "quotient_filter",
        [ROOT / "mojo" / "src" / "db_structures" / "quotient_filter.mojo"],
    ),
    (
        "mojo",
        "elias_fano",
        [ROOT / "mojo" / "src" / "db_structures" / "elias_fano.mojo"],
    ),
]

COMMENT_PREFIX = {
    ".hpp": "//",
    ".cpp": "//",
    ".mojo": "#",
    ".py": "#",
}

TOKEN_PATTERN = re.compile(
    r"[A-Za-z_][A-Za-z0-9_]*|0x[0-9A-Fa-f]+|\d+|==|!=|<=|>=|&&|\|\||<<|>>|[+\-*/%&|^<>=]"
)
CONTROL_FLOW_PATTERN = re.compile(r"\b(?:if|else\s+if|elif|for|while|switch|catch)\b")

UNSAFE_PATTERNS = [
    re.compile(r"\bunsafe\b"),
    re.compile(r"\bpointer\b", re.IGNORECASE),
    re.compile(r"\breinterpret_cast\b"),
    re.compile(r"\bUnsafePointer\b"),
]

BIT_PATTERNS = [
    re.compile(r"\b(?:bit_mask|bit_offset|bit_position|bit_index|bit_count)\b"),
    re.compile(
        r"\b(?:lower_mask|remainder_mask|index_mask|occupied_mask|continuation_mask|shifted_mask)\b"
    ),
    re.compile(r"(?:<<|>>|&\s*\(|&\s*0x|\|\s*0x)"),
    re.compile(r"\b(?:popcount|countr_zero|pop_count|trailing_zero)\b"),
]


def count_loc(path: Path) -> int:
    prefix = COMMENT_PREFIX.get(path.suffix, "//")
    total = 0
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if stripped and not stripped.startswith(prefix):
            total += 1
    return total


def strip_comments(text: str, suffix: str) -> str:
    if suffix in {".hpp", ".cpp"}:
        return re.sub(r"//.*", "", text)
    return re.sub(r"#.*", "", text)


def count_tokens(path: Path) -> int:
    text = strip_comments(path.read_text(encoding="utf-8"), path.suffix)
    return len(TOKEN_PATTERN.findall(text))


def count_pattern_matches(path: Path, patterns: list[re.Pattern[str]]) -> int:
    text = path.read_text(encoding="utf-8")
    total = 0
    for pattern in patterns:
        total += len(pattern.findall(text))
    return total


def helper_blocks(path: Path) -> list[str]:
    if path.suffix in {".hpp", ".cpp"}:
        functions, _ = parse_cpp_symbols(path)
    else:
        functions, _ = parse_mojo_symbols(path)
    return [block.text for block in functions.values()]


def helper_token_metrics(path: Path) -> tuple[int, float, int]:
    blocks = helper_blocks(path)
    helper_count = len(blocks)
    if helper_count == 0:
        return 0, 0.0, 0
    token_counts = [
        len(TOKEN_PATTERN.findall(strip_comments(block, path.suffix)))
        for block in blocks
    ]
    return helper_count, sum(token_counts) / helper_count, max(token_counts)


def control_flow_sites(path: Path) -> int:
    text = strip_comments(path.read_text(encoding="utf-8"), path.suffix)
    return len(CONTROL_FLOW_PATTERN.findall(text))


def main() -> None:
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(
            [
                "language",
                "scope",
                "files",
                "loc_no_comments",
                "token_count",
                "helper_count",
                "avg_helper_tokens",
                "max_helper_tokens",
                "control_flow_sites",
                "unsafe_or_pointer_sites",
                "bit_packing_sites",
            ]
        )
        for language, scope, files in FILE_GROUPS:
            files = sorted(path for path in files if path.is_file())
            helper_count_total = 0
            helper_token_sum = 0.0
            max_helper_tokens = 0
            for path in files:
                helper_count, avg_helper_tokens, path_max_helper_tokens = helper_token_metrics(path)
                helper_count_total += helper_count
                helper_token_sum += avg_helper_tokens * helper_count
                max_helper_tokens = max(max_helper_tokens, path_max_helper_tokens)

            avg_helper_tokens_total = (
                helper_token_sum / helper_count_total if helper_count_total > 0 else 0.0
            )
            writer.writerow(
                [
                    language,
                    scope,
                    len(files),
                    sum(count_loc(path) for path in files),
                    sum(count_tokens(path) for path in files),
                    helper_count_total,
                    f"{avg_helper_tokens_total:.3f}",
                    max_helper_tokens,
                    sum(control_flow_sites(path) for path in files),
                    sum(
                        count_pattern_matches(path, UNSAFE_PATTERNS)
                        for path in files
                    ),
                    sum(
                        count_pattern_matches(path, BIT_PATTERNS)
                        for path in files
                    ),
                ]
            )
    print(f"Wrote code metrics to {OUTPUT}")


if __name__ == "__main__":
    main()
