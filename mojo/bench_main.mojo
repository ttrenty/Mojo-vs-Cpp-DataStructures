from std.memory import Span
from std.sys import argv
from std.time import perf_counter_ns

from db_structures.benchmark_utils import (
    default_negative_queries,
    default_positive_keys,
    density_label,
    make_mixed_queries,
    make_sorted_unique_values,
    ns_per_op,
)
from db_structures.blocked_bloom import BlockedBloomFilter, empirical_false_positive_rate
from db_structures.elias_fano import EliasFano
from db_structures.hash import recommended_k_hashes
from db_structures.quotient_filter import QuotientFilter, create_quotient_filter


def get_arg(
    args: Span[StaticString, StaticConstantOrigin],
    name: StaticString,
    default_value: StaticString,
) -> StaticString:
    for index in range(2, len(args) - 1):
        if args[index] == name:
            return args[index + 1]
    return default_value


def emit_header():
    print(
        "language,structure,workload,dataset,n,param_tuple,run_id,throughput_ns_per_op,"
        "memory_bytes,fpr,compile_time_ms,git_hash,compiler_version,observed_hits,"
        "observed_checksum,avg_probe_distance,max_probe_distance,max_cluster_length,"
        "avg_cluster_length"
    )


def format_load(value: Float64) -> String:
    var scaled = Int(value * 100.0 + 0.5)
    var whole = scaled // 100
    var fraction = scaled % 100
    return String.write(t"{whole}.{fraction // 10}{fraction % 10}")


def emit_row(
    structure: StaticString,
    workload: StaticString,
    dataset: StaticString,
    n: Int,
    param_tuple: String,
    run_id: Int,
    time_ns_per_op: Float64,
    memory_bytes: Int,
    fpr: Float64,
    compile_time_ms: Int,
    git_hash: StaticString,
    observed_hits: Int = -1,
    observed_checksum: UInt64 = 0,
    avg_probe_distance: Float64 = -1.0,
    max_probe_distance: Int = -1,
    max_cluster_length: Int = -1,
    avg_cluster_length: Float64 = -1.0,
):
    print(
        t"mojo,{structure},{workload},{dataset},{n},{param_tuple},{run_id},{time_ns_per_op},{memory_bytes},{fpr},{compile_time_ms},{git_hash},mojo,{observed_hits},{observed_checksum},{avg_probe_distance},{max_probe_distance},{max_cluster_length},{avg_cluster_length}"
    )


def blocked_bloom_param_tuple(bits_per_key: Int, k_hashes: Int, query_mode: StaticString) -> String:
    return String.write(
        t"bits_per_key={bits_per_key};k={k_hashes};query_mode={query_mode}"
    )


def quotient_filter_param_tuple(q: Int, remainder_bits: Int, target_load: Float64) -> String:
    var load_text = format_load(target_load)
    return String.write(t"q={q};remainder_bits={remainder_bits};target_load={load_text}")


def elias_fano_param_tuple(universe_factor: Int, dataset: StaticString, encoded: EliasFano) -> String:
    return String.write(t"universe_factor={universe_factor};density={dataset};lower_bits={encoded.lower_bits}")


def run_blocked_bloom_contains(filter: BlockedBloomFilter, queries: List[UInt64]) -> Int:
    var hits = 0
    for query in queries:
        if filter.contains(query):
            hits += 1
    return hits


def run_quotient_read_heavy(
    filter: QuotientFilter,
    positives: List[UInt64],
    negatives: List[UInt64],
    present_count: Int,
    op_count: Int,
) -> Int:
    var hits = 0
    for index in range(op_count):
        if filter.contains(positives[index % present_count]):
            hits += 1
        if filter.contains(negatives[index % present_count]):
            hits += 1
    return hits


def run_quotient_mixed(
    mut filter: QuotientFilter,
    positives: List[UInt64],
    negatives: List[UInt64],
    initial_count: Int,
    op_count: Int,
) -> Int:
    var hits = 0
    var insert_cursor = initial_count
    var erase_cursor = 0
    for step in range(op_count):
        var phase = step % 20
        if phase < 9:
            var query = (
                positives[step % initial_count]
                if phase % 2 == 0
                else negatives[step % len(negatives)]
            )
            if filter.contains(query):
                hits += 1
        elif phase < 18:
            _ = filter.insert(positives[insert_cursor % len(positives)])
            insert_cursor += 1
        else:
            _ = filter.erase(positives[erase_cursor % initial_count])
            erase_cursor += 1
    return hits


def run_quotient_delete_queries(filter: QuotientFilter, positives: List[UInt64], count: Int) -> Int:
    var hits = 0
    for index in range(count):
        if filter.contains(positives[index]):
            hits += 1
    return hits


def run_elias_fano_contains(encoded: EliasFano, values: List[UInt64], op_count: Int) -> Int:
    var hits = 0
    for index in range(op_count):
        var query = values[index % len(values)] if index % 2 == 0 else values[index % len(values)] + UInt64(1)
        if encoded.contains(query):
            hits += 1
    return hits


def run_elias_fano_select_checksum(encoded: EliasFano, value_count: Int, op_count: Int) -> UInt64:
    var checksum = UInt64(0)
    for index in range(op_count):
        checksum = checksum ^ encoded.select(index % value_count)
    return checksum


def run_elias_fano_predecessor(encoded: EliasFano, values: List[UInt64], op_count: Int) -> Int:
    var hits = 0
    for index in range(op_count):
        var predecessor = encoded.predecessor(values[index % len(values)] + UInt64(3))
        if predecessor != None:
            hits += 1
    return hits


def run_blocked_bloom_bench(args: Span[StaticString, StaticConstantOrigin]) raises:
    var n = Int(atol(get_arg(args, "--n", "100000")))
    var bits_per_key = Int(atol(get_arg(args, "--bits-per-key", "10")))
    var runs = Int(atol(get_arg(args, "--runs", "7")))
    var warmup_runs = Int(atol(get_arg(args, "--warmup-runs", "2")))
    var query_mode = get_arg(args, "--query-mode", "negative")
    var git_hash = get_arg(args, "--git-hash", "unknown")
    var compile_time_ms = Int(atol(get_arg(args, "--compile-time-ms", "-1")))
    var k_hashes = recommended_k_hashes(bits_per_key)

    var keys = default_positive_keys(n)
    var negatives = default_negative_queries(n)
    var mixed_queries = make_mixed_queries(keys, negatives)
    var use_mixed = query_mode == "mixed"

    emit_header()

    for _ in range(warmup_runs):
        var warm_filter = BlockedBloomFilter(n, bits_per_key, k_hashes)
        for key in keys:
            warm_filter.insert(key)
        _ = warm_filter.memory_bytes()

    for run_id in range(runs):
        var start_ns = perf_counter_ns()
        var filter = BlockedBloomFilter(n, bits_per_key, k_hashes)
        for key in keys:
            filter.insert(key)
        var end_ns = perf_counter_ns()
        emit_row(
            "blocked_bloom",
            "build",
            "dense",
            n,
            blocked_bloom_param_tuple(bits_per_key, k_hashes, query_mode),
            run_id,
            ns_per_op(end_ns - start_ns, len(keys)),
            filter.memory_bytes(),
            0.0,
            compile_time_ms,
            git_hash,
        )

    var filter = BlockedBloomFilter.build(keys, bits_per_key, k_hashes)
    for key in keys:
        if not filter.contains(key):
            raise Error("false negative during Blocked Bloom sanity pass")

    var fpr = empirical_false_positive_rate(filter, negatives)
    for _ in range(warmup_runs):
        if use_mixed:
            _ = run_blocked_bloom_contains(filter, mixed_queries)
        else:
            _ = run_blocked_bloom_contains(filter, negatives)

    for run_id in range(runs):
        var start_ns = perf_counter_ns()
        if use_mixed:
            var hits = run_blocked_bloom_contains(filter, mixed_queries)
            var end_ns = perf_counter_ns()
            emit_row(
                "blocked_bloom",
                "contains_mixed",
                "dense",
                n,
                blocked_bloom_param_tuple(bits_per_key, k_hashes, query_mode),
                run_id,
                ns_per_op(end_ns - start_ns, len(mixed_queries)),
                filter.memory_bytes(),
                fpr,
                compile_time_ms,
                git_hash,
                observed_hits=hits,
            )
        else:
            var hits = run_blocked_bloom_contains(filter, negatives)
            var end_ns = perf_counter_ns()
            emit_row(
                "blocked_bloom",
                "contains_negative",
                "dense",
                n,
                blocked_bloom_param_tuple(bits_per_key, k_hashes, query_mode),
                run_id,
                ns_per_op(end_ns - start_ns, len(negatives)),
                filter.memory_bytes(),
                fpr,
                compile_time_ms,
                git_hash,
                observed_hits=hits,
            )


def run_quotient_filter_bench(args: Span[StaticString, StaticConstantOrigin]) raises:
    var q = Int(atol(get_arg(args, "--q", "16")))
    var remainder_bits = Int(atol(get_arg(args, "--remainder-bits", "12")))
    var runs = Int(atol(get_arg(args, "--runs", "7")))
    var warmup_runs = Int(atol(get_arg(args, "--warmup-runs", "2")))
    var op_count = Int(atol(get_arg(args, "--op-count", "40000")))
    var target_load = atof(get_arg(args, "--target-load", "0.70"))
    var workload = get_arg(args, "--workload", "read_heavy")
    var git_hash = get_arg(args, "--git-hash", "unknown")
    var compile_time_ms = Int(atol(get_arg(args, "--compile-time-ms", "-1")))

    var capacity = Int(1) << q
    var target_count = Int(Float64(capacity) * target_load)
    if target_count < 1:
        target_count = 1

    var keys = default_positive_keys(target_count + op_count * 2)
    var negatives = default_negative_queries(target_count + op_count * 2)

    emit_header()

    for _ in range(warmup_runs):
        var warm_filter = create_quotient_filter(q, remainder_bits)
        for index in range(target_count):
            _ = warm_filter.insert(keys[index])
        _ = warm_filter.memory_bytes()

    for run_id in range(runs):
        var built = create_quotient_filter(q, remainder_bits)
        var start_ns = perf_counter_ns()
        for index in range(target_count):
            _ = built.insert(keys[index])
        var end_ns = perf_counter_ns()
        emit_row(
            "quotient_filter",
            "build_insert",
            "dense",
            target_count,
            quotient_filter_param_tuple(q, remainder_bits, target_load),
            run_id,
            ns_per_op(end_ns - start_ns, target_count),
            built.memory_bytes(),
            0.0,
            compile_time_ms,
            git_hash,
            avg_probe_distance=built.average_probe_distance(),
            max_probe_distance=built.max_probe_distance(),
            max_cluster_length=built.max_cluster_length(),
            avg_cluster_length=built.average_cluster_length(),
        )

    var baseline = create_quotient_filter(q, remainder_bits)
    for index in range(target_count):
        _ = baseline.insert(keys[index])
    for index in range(0, target_count, target_count // 64 if target_count > 64 else 1):
        if not baseline.contains(keys[index]):
            raise Error("Quotient Filter sanity check failed")

    var negative_hits = 0
    for index in range(target_count):
        if baseline.contains(negatives[index]):
            negative_hits += 1
    var fpr = Float64(negative_hits) / Float64(target_count)

    if workload == "read_heavy":
        for _ in range(warmup_runs):
            _ = run_quotient_read_heavy(baseline, keys, negatives, target_count, op_count)
        for run_id in range(runs):
            var start_ns = perf_counter_ns()
            var hits = run_quotient_read_heavy(
                baseline, keys, negatives, target_count, op_count
            )
            var end_ns = perf_counter_ns()
            emit_row(
                "quotient_filter",
                "read_heavy",
                "dense",
                target_count,
                quotient_filter_param_tuple(q, remainder_bits, target_load),
                run_id,
                ns_per_op(end_ns - start_ns, op_count * 2),
                baseline.memory_bytes(),
                fpr,
                compile_time_ms,
                git_hash,
                observed_hits=hits,
                avg_probe_distance=baseline.average_probe_distance(),
                max_probe_distance=baseline.max_probe_distance(),
                max_cluster_length=baseline.max_cluster_length(),
                avg_cluster_length=baseline.average_cluster_length(),
            )
    elif workload == "mixed":
        var initial_count = target_count // 2
        if initial_count < 1:
            initial_count = 1
        for _ in range(warmup_runs):
            var warm_filter = create_quotient_filter(q, remainder_bits)
            for index in range(initial_count):
                _ = warm_filter.insert(keys[index])
            _ = run_quotient_mixed(warm_filter, keys, negatives, initial_count, op_count)
        for run_id in range(runs):
            var filter = create_quotient_filter(q, remainder_bits)
            for index in range(initial_count):
                _ = filter.insert(keys[index])
            var start_ns = perf_counter_ns()
            var hits = run_quotient_mixed(filter, keys, negatives, initial_count, op_count)
            var end_ns = perf_counter_ns()
            emit_row(
                "quotient_filter",
                "mixed_ops",
                "dense",
                target_count,
                quotient_filter_param_tuple(q, remainder_bits, target_load),
                run_id,
                ns_per_op(end_ns - start_ns, op_count),
                filter.memory_bytes(),
                fpr,
                compile_time_ms,
                git_hash,
                observed_hits=hits,
                avg_probe_distance=filter.average_probe_distance(),
                max_probe_distance=filter.max_probe_distance(),
                max_cluster_length=filter.max_cluster_length(),
                avg_cluster_length=filter.average_cluster_length(),
            )
    elif workload == "delete_heavy":
        for _ in range(warmup_runs):
            var warm_filter = baseline.copy()
            for index in range(target_count // 2):
                _ = warm_filter.erase(keys[index])
            _ = warm_filter.memory_bytes()
        for _ in range(warmup_runs):
            var warm_filter = baseline.copy()
            for index in range(target_count // 2):
                _ = warm_filter.erase(keys[index])
            _ = run_quotient_delete_queries(warm_filter, keys, target_count)
        for run_id in range(runs):
            var filter = baseline.copy()
            var start_ns = perf_counter_ns()
            for index in range(target_count // 2):
                _ = filter.erase(keys[index])
            var end_ns = perf_counter_ns()
            emit_row(
                "quotient_filter",
                "erase_delete_heavy",
                "dense",
                target_count,
                quotient_filter_param_tuple(q, remainder_bits, target_load),
                run_id,
                ns_per_op(end_ns - start_ns, target_count // 2 if target_count > 1 else 1),
                filter.memory_bytes(),
                fpr,
                compile_time_ms,
                git_hash,
                observed_hits=0,
                avg_probe_distance=filter.average_probe_distance(),
                max_probe_distance=filter.max_probe_distance(),
                max_cluster_length=filter.max_cluster_length(),
                avg_cluster_length=filter.average_cluster_length(),
            )

            var query_start_ns = perf_counter_ns()
            var hits = run_quotient_delete_queries(filter, keys, target_count)
            var query_end_ns = perf_counter_ns()
            emit_row(
                "quotient_filter",
                "contains_delete_heavy",
                "dense",
                target_count,
                quotient_filter_param_tuple(q, remainder_bits, target_load),
                run_id,
                ns_per_op(query_end_ns - query_start_ns, target_count),
                filter.memory_bytes(),
                fpr,
                compile_time_ms,
                git_hash,
                observed_hits=hits,
                avg_probe_distance=filter.average_probe_distance(),
                max_probe_distance=filter.max_probe_distance(),
                max_cluster_length=filter.max_cluster_length(),
                avg_cluster_length=filter.average_cluster_length(),
            )
    else:
        raise Error("unknown quotient_filter workload")


def run_elias_fano_bench(args: Span[StaticString, StaticConstantOrigin]) raises:
    var n = Int(atol(get_arg(args, "--n", "100000")))
    var universe_factor = Int(atol(get_arg(args, "--universe-factor", "256")))
    var runs = Int(atol(get_arg(args, "--runs", "7")))
    var warmup_runs = Int(atol(get_arg(args, "--warmup-runs", "2")))
    var op_count = Int(atol(get_arg(args, "--op-count", "40000")))
    var workload = get_arg(args, "--workload", "contains")
    var git_hash = get_arg(args, "--git-hash", "unknown")
    var compile_time_ms = Int(atol(get_arg(args, "--compile-time-ms", "-1")))
    var dataset = density_label(universe_factor)

    var values = make_sorted_unique_values(n, universe_factor)
    emit_header()

    for _ in range(warmup_runs):
        var warm_encoded = EliasFano.build(values, values[len(values) - 1])
        _ = warm_encoded.memory_bytes()

    for run_id in range(runs):
        var start_ns = perf_counter_ns()
        var encoded = EliasFano.build(values, values[len(values) - 1])
        var end_ns = perf_counter_ns()
        emit_row(
            "elias_fano",
            "build",
            dataset,
            n,
            elias_fano_param_tuple(universe_factor, dataset, encoded),
            run_id,
            ns_per_op(end_ns - start_ns, len(values)),
            encoded.memory_bytes(),
            0.0,
            compile_time_ms,
            git_hash,
        )

    var encoded = EliasFano.build(values, values[len(values) - 1])
    for index in range(0, len(values), len(values) // 64 if len(values) > 64 else 1):
        if encoded.select(index) != values[index]:
            raise Error("Elias-Fano select sanity check failed")

    if workload == "contains":
        for _ in range(warmup_runs):
            _ = run_elias_fano_contains(encoded, values, op_count)
        for run_id in range(runs):
            var start_ns = perf_counter_ns()
            var hits = run_elias_fano_contains(encoded, values, op_count)
            var end_ns = perf_counter_ns()
            emit_row(
                "elias_fano",
                "contains",
                dataset,
                n,
                elias_fano_param_tuple(universe_factor, dataset, encoded),
                run_id,
                ns_per_op(end_ns - start_ns, op_count),
                encoded.memory_bytes(),
                0.0,
                compile_time_ms,
                git_hash,
                observed_hits=hits,
            )
    elif workload == "select":
        for _ in range(warmup_runs):
            _ = run_elias_fano_select_checksum(encoded, len(values), op_count)
        for run_id in range(runs):
            var start_ns = perf_counter_ns()
            var checksum = run_elias_fano_select_checksum(encoded, len(values), op_count)
            var end_ns = perf_counter_ns()
            emit_row(
                "elias_fano",
                "select",
                dataset,
                n,
                elias_fano_param_tuple(universe_factor, dataset, encoded),
                run_id,
                ns_per_op(end_ns - start_ns, op_count),
                encoded.memory_bytes(),
                0.0,
                compile_time_ms,
                git_hash,
                observed_checksum=checksum,
            )
    elif workload == "predecessor":
        for _ in range(warmup_runs):
            _ = run_elias_fano_predecessor(encoded, values, op_count)
        for run_id in range(runs):
            var start_ns = perf_counter_ns()
            var hits = run_elias_fano_predecessor(encoded, values, op_count)
            var end_ns = perf_counter_ns()
            emit_row(
                "elias_fano",
                "predecessor",
                dataset,
                n,
                elias_fano_param_tuple(universe_factor, dataset, encoded),
                run_id,
                ns_per_op(end_ns - start_ns, op_count),
                encoded.memory_bytes(),
                0.0,
                compile_time_ms,
                git_hash,
                observed_hits=hits,
            )
    else:
        raise Error("unknown elias_fano workload")


def main() raises:
    var args = argv()
    if len(args) < 2:
        print("usage: mojo_bench <blocked_bloom|quotient_filter|elias_fano> [options]")
        return

    if args[1] == "blocked_bloom":
        run_blocked_bloom_bench(args)
        return
    if args[1] == "quotient_filter":
        run_quotient_filter_bench(args)
        return
    if args[1] == "elias_fano":
        run_elias_fano_bench(args)
        return

    raise Error("unknown mojo_bench subcommand")
