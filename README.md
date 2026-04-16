# Mojo vs C++ Data Structures

This repository compares matched Mojo and C++ implementations of three
database-oriented data structures from CSC2525:

- Blocked Bloom Filter
- Quotient Filter
- Elias-Fano

The goal is simple: keep the two implementations as comparable as possible,
then measure where they actually differ in performance and code structure.

Side-by-side HTML comparison of Mojo and C++ code: [https://ttrenty.github.io/Mojo-vs-Cpp-DataStructures/](https://ttrenty.github.io/Mojo-vs-Cpp-DataStructures/)

## Repository Layout

```text
common/   shared scripts, benchmark params, plotting, compare tool, hash spec
cpp/      C++ implementation, tests, benchmarks, and small helper CLIs
mojo/     Mojo implementation, tests, benchmarks, and small helper CLIs
results/  raw results, processed summaries, generated figures, compare artifacts
```

## Quick Start

The project uses Pixi from the repository root.

```bash
pixi run tests
pixi run bench_tests
pixi run compare_impls
pixi run bench_full # or bench_full_parallel to run tasks in parallel across CPU cores
pixi run plot_results
```

If you need a different C++ compiler:

```bash
CXX=g++ pixi run _build_cpp
```

## Most Useful Commands

- `pixi run tests`
  - correctness tests, hash conformance, and cross-language structure parity
- `pixi run bench_tests`
  - quick smoke benchmark check
- `pixi run compare_impls`
  - generate the C++/Mojo comparison report in HTML, Markdown, and JSON
- `pixi run compare_impls_check`
  - fail if the comparison contract detects drift
- `pixi run bench_full`
  - run the main benchmark set used for the report
- `pixi run bench_full_parallel`
  - same benchmark set, but spread across most CPU cores while leaving a few free
- `pixi run plot_results`
  - summarize the full results and regenerate the report-facing figures
- `pixi run plot_results_full_parallel`
  - summarize and plot the parallel full run separately

You can pass extra benchmark arguments through Pixi with `--`:

```bash
pixi run bench_full -- --runs-override 15 --warmup-runs-override 5
pixi run bench_full_parallel -- --reserve-cores 3 --runs-override 15
```

## Results Layout

Single-job and parallel runs are kept separate on purpose.

Raw outputs:

- `results/raw/full/`
- `results/raw/full_parallel/`
- `results/raw/representative/`
- `results/raw/representative_parallel/`
- `results/raw/smoke/`

Processed summaries:

- `results/processed/full_summary.csv`
- `results/processed/full_parallel_summary.csv`
- `results/processed/code_metrics.csv`
- `results/processed/build_metrics.csv`

Figures:

- `results/figures/full/`
- `results/figures/full_parallel/`

Comparison artifacts:

- `results/processed/implementation_compare.html`
- `results/processed/implementation_compare.md`
- `results/processed/implementation_compare.json`

## What Is Shared Across Languages?

To keep the comparison fair, the two implementations share:

- hash constants and deterministic seeds
- deterministic dataset generation
- benchmark workloads
- helper-level comparison rules
- structure-parity checks on raw internal state

The shared hash spec lives in [common/hash_spec.json](./common/hash_spec.json).
Generated bindings are written into `build/generated/`.

## Notes

- Benchmarks are pinned to one CPU core when possible.
- Reported timings use warmup runs first, then timed repetitions.
- The line plots show median throughput with an interquartile band.
