from __future__ import annotations

import argparse
import json
from pathlib import Path

from hash_reference import dataset_key, negative_query

UNIVERSE_FACTORS = {
    "dense": 16,
    "medium": 256,
    "sparse": 4096,
}


def build_dataset(n: int, density: str) -> dict[str, object]:
    factor = UNIVERSE_FACTORS[density]
    keys = [dataset_key(i) % (factor * n) for i in range(n)]
    negatives = [negative_query(i) % (factor * n) + factor * n for i in range(n)]
    return {
        "density": density,
        "n": n,
        "universe_max": factor * n,
        "keys": keys,
        "negative_queries": negatives,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Generate deterministic datasets.")
    parser.add_argument("--n", type=int, required=True)
    parser.add_argument("--density", choices=sorted(UNIVERSE_FACTORS), required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    payload = build_dataset(args.n, args.density)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
    print(f"Wrote dataset to {args.output}")


if __name__ == "__main__":
    main()
