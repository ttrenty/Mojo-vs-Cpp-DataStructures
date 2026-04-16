from __future__ import annotations

import json
import subprocess
from pathlib import Path

from hash_reference import dataset_key, negative_query, recommended_k_hashes

ROOT = Path(__file__).resolve().parents[1]
GOLDEN_PATH = ROOT / "build" / "generated" / "hash_golden.json"
CPP_BIN = ROOT / "build" / "cpp" / "cpp_hash"
MOJO_BIN = ROOT / "build" / "mojo" / "mojo_hash"


def run_scalar(command: list[str]) -> int:
    completed = subprocess.run(
        command,
        cwd=ROOT,
        capture_output=True,
        check=True,
        text=True,
    )
    return int(completed.stdout.strip())


def verify_helper_parity(failures: list[str]) -> None:
    for index in range(32):
        expected_dataset_key = dataset_key(index)
        cpp_dataset_key = run_scalar([str(CPP_BIN), "dataset_key", str(index)])
        mojo_dataset_key = run_scalar([str(MOJO_BIN), "dataset_key", str(index)])
        if cpp_dataset_key != expected_dataset_key:
            failures.append(
                f"C++ dataset_key mismatch for index {index}: expected {expected_dataset_key}, got {cpp_dataset_key}"
            )
        if mojo_dataset_key != expected_dataset_key:
            failures.append(
                f"Mojo dataset_key mismatch for index {index}: expected {expected_dataset_key}, got {mojo_dataset_key}"
            )

        expected_negative_query = negative_query(index)
        cpp_negative_query = run_scalar([str(CPP_BIN), "negative_query", str(index)])
        mojo_negative_query = run_scalar([str(MOJO_BIN), "negative_query", str(index)])
        if cpp_negative_query != expected_negative_query:
            failures.append(
                f"C++ negative_query mismatch for index {index}: expected {expected_negative_query}, got {cpp_negative_query}"
            )
        if mojo_negative_query != expected_negative_query:
            failures.append(
                f"Mojo negative_query mismatch for index {index}: expected {expected_negative_query}, got {mojo_negative_query}"
            )

    for bits_per_key in range(0, 257):
        expected = recommended_k_hashes(bits_per_key)
        cpp_value = run_scalar([str(CPP_BIN), "recommended_k_hashes", str(bits_per_key)])
        mojo_value = run_scalar([str(MOJO_BIN), "recommended_k_hashes", str(bits_per_key)])
        if cpp_value != expected:
            failures.append(
                f"C++ recommended_k_hashes mismatch for bits_per_key {bits_per_key}: expected {expected}, got {cpp_value}"
            )
        if mojo_value != expected:
            failures.append(
                f"Mojo recommended_k_hashes mismatch for bits_per_key {bits_per_key}: expected {expected}, got {mojo_value}"
            )


def main() -> None:
    payload = json.loads(GOLDEN_PATH.read_text(encoding="utf-8"))
    failures: list[str] = []

    for case in payload["cases"]:
        key = str(case["key"])
        seed = str(case["seed"])
        expected = int(case["hash"])
        cpp_value = run_scalar([str(CPP_BIN), "hash", key, seed])
        mojo_value = run_scalar([str(MOJO_BIN), key, seed])

        if cpp_value != expected:
            failures.append(
                f"C++ mismatch for case {case['index']}: expected {expected}, got {cpp_value}"
            )
        if mojo_value != expected:
            failures.append(
                f"Mojo mismatch for case {case['index']}: expected {expected}, got {mojo_value}"
            )

    verify_helper_parity(failures)

    if failures:
        raise SystemExit("\n".join(failures))

    print(
        f"Validated {len(payload['cases'])} hash cases plus helper parity across C++ and Mojo."
    )


if __name__ == "__main__":
    main()
