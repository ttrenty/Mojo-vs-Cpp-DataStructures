from __future__ import annotations

import subprocess
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CPP_BIN = ROOT / "build" / "cpp" / "cpp_structure_parity"
MOJO_BIN = ROOT / "build" / "mojo" / "mojo_structure_parity"
STRUCTURES = ("blocked_bloom", "quotient_filter", "elias_fano")


def run_export(command: list[str]) -> dict[str, list[str]]:
    completed = subprocess.run(
        command,
        cwd=ROOT,
        capture_output=True,
        check=True,
        text=True,
    )
    payload: dict[str, list[str]] = defaultdict(list)
    for raw_line in completed.stdout.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        if "=" not in line:
            raise SystemExit(f"Malformed parity output line: {line!r}")
        key, value = line.split("=", 1)
        payload[key].append(value)
    return dict(payload)


def diff_payloads(
    structure: str,
    cpp_payload: dict[str, list[str]],
    mojo_payload: dict[str, list[str]],
) -> list[str]:
    failures: list[str] = []
    all_keys = sorted(set(cpp_payload) | set(mojo_payload))
    for key in all_keys:
        cpp_values = cpp_payload.get(key)
        mojo_values = mojo_payload.get(key)
        if cpp_values != mojo_values:
            failures.append(
                f"{structure}: mismatch for {key}: C++={cpp_values!r} Mojo={mojo_values!r}"
            )
    return failures


def main() -> None:
    failures: list[str] = []
    for structure in STRUCTURES:
        cpp_payload = run_export([str(CPP_BIN), structure])
        mojo_payload = run_export([str(MOJO_BIN), structure])
        failures.extend(diff_payloads(structure, cpp_payload, mojo_payload))

    if failures:
        raise SystemExit("\n".join(failures))

    joined = ", ".join(STRUCTURES)
    print(f"Validated cross-language structure parity for {joined}.")


if __name__ == "__main__":
    main()
