#include <cstdlib>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>
#include <string_view>
#include <vector>

#include "benchmark_utils.hpp"
#include "blocked_bloom.hpp"
#include "elias_fano.hpp"
#include "quotient_filter.hpp"

namespace {

struct Options {
    std::size_t n = 100'000U;
    std::size_t bits_per_key = 10U;
    std::size_t runs = 7U;
    std::size_t warmup_runs = 2U;
    std::size_t q = 16U;
    std::size_t remainder_bits = 12U;
    std::size_t universe_factor = 256U;
    std::size_t op_count = 40'000U;
    double target_load = 0.70;
    std::string query_mode = "negative";
    std::string workload = "read_heavy";
    std::string git_hash = dsw::env_or_default("GIT_HASH", "unknown");
    int compile_time_ms = -1;
};

struct Telemetry {
    long long observed_hits = -1;
    unsigned long long observed_checksum = 0ULL;
    double avg_probe_distance = -1.0;
    long long max_probe_distance = -1;
    long long max_cluster_length = -1;
    double avg_cluster_length = -1.0;
};

template <typename Fn>
void repeat_warmup(std::size_t warmup_runs, Fn&& fn) {
    for (std::size_t warmup_id = 0; warmup_id < warmup_runs; ++warmup_id) {
        fn();
    }
}

[[nodiscard]] std::size_t run_blocked_bloom_contains(
    const dsw::BlockedBloomFilter& filter,
    std::span<const std::uint64_t> queries
) {
    std::size_t hits = 0U;
    for (const auto query : queries) {
        hits += static_cast<std::size_t>(filter.contains(query));
    }
    return hits;
}

[[nodiscard]] std::size_t run_quotient_read_heavy(
    const dsw::QuotientFilter& filter,
    std::span<const std::uint64_t> positives,
    std::span<const std::uint64_t> negatives,
    std::size_t present_count,
    std::size_t op_count
) {
    std::size_t hits = 0U;
    for (std::size_t index = 0; index < op_count; ++index) {
        hits += static_cast<std::size_t>(filter.contains(positives[index % present_count]));
        hits += static_cast<std::size_t>(filter.contains(negatives[index % present_count]));
    }
    return hits;
}

[[nodiscard]] std::size_t run_quotient_mixed(
    dsw::QuotientFilter& filter,
    std::span<const std::uint64_t> positives,
    std::span<const std::uint64_t> negatives,
    std::size_t initial_count,
    std::size_t op_count
) {
    std::size_t hits = 0U;
    std::size_t insert_cursor = initial_count;
    std::size_t erase_cursor = 0U;
    for (std::size_t step = 0; step < op_count; ++step) {
        const std::size_t phase = step % 20U;
        if (phase < 9U) {
            const auto query = phase % 2U == 0U ? positives[step % initial_count]
                                                : negatives[step % negatives.size()];
            hits += static_cast<std::size_t>(filter.contains(query));
        } else if (phase < 18U) {
            (void)filter.insert(positives[insert_cursor % positives.size()]);
            ++insert_cursor;
        } else {
            (void)filter.erase(positives[erase_cursor % initial_count]);
            ++erase_cursor;
        }
    }
    return hits;
}

[[nodiscard]] std::size_t run_quotient_delete_queries(
    const dsw::QuotientFilter& filter,
    std::span<const std::uint64_t> positives,
    std::size_t count
) {
    std::size_t hits = 0U;
    for (std::size_t index = 0; index < count; ++index) {
        hits += static_cast<std::size_t>(filter.contains(positives[index]));
    }
    return hits;
}

[[nodiscard]] std::size_t run_elias_fano_contains(
    const dsw::EliasFano& encoded,
    std::span<const std::uint64_t> values,
    std::size_t op_count
) {
    std::size_t hits = 0U;
    for (std::size_t index = 0; index < op_count; ++index) {
        const std::uint64_t query =
            index % 2U == 0U ? values[index % values.size()] : values[index % values.size()] + 1U;
        hits += static_cast<std::size_t>(encoded.contains(query));
    }
    return hits;
}

[[nodiscard]] std::uint64_t run_elias_fano_select_checksum(
    const dsw::EliasFano& encoded,
    std::size_t value_count,
    std::size_t op_count
) {
    std::uint64_t checksum = 0U;
    for (std::size_t index = 0; index < op_count; ++index) {
        checksum ^= encoded.select(index % value_count);
    }
    return checksum;
}

[[nodiscard]] std::size_t run_elias_fano_predecessor(
    const dsw::EliasFano& encoded,
    std::span<const std::uint64_t> values,
    std::size_t op_count
) {
    std::size_t hits = 0U;
    for (std::size_t index = 0; index < op_count; ++index) {
        const auto predecessor = encoded.predecessor(values[index % values.size()] + 3U);
        hits += static_cast<std::size_t>(predecessor.has_value());
    }
    return hits;
}

[[nodiscard]] std::string_view get_value(
    int argc,
    char** argv,
    std::string_view name,
    std::string_view fallback
) {
    for (int index = 2; index + 1 < argc; ++index) {
        if (std::string_view(argv[index]) == name) {
            return argv[index + 1];
        }
    }
    return fallback;
}

[[nodiscard]] Options parse_options(int argc, char** argv) {
    Options options;
    options.n = static_cast<std::size_t>(
        std::stoull(std::string(get_value(argc, argv, "--n", "100000")))
    );
    options.bits_per_key = static_cast<std::size_t>(
        std::stoull(std::string(get_value(argc, argv, "--bits-per-key", "10")))
    );
    options.runs = static_cast<std::size_t>(
        std::stoull(std::string(get_value(argc, argv, "--runs", "7")))
    );
    options.warmup_runs = static_cast<std::size_t>(
        std::stoull(std::string(get_value(argc, argv, "--warmup-runs", "2")))
    );
    options.q = static_cast<std::size_t>(
        std::stoull(std::string(get_value(argc, argv, "--q", "16")))
    );
    options.remainder_bits = static_cast<std::size_t>(
        std::stoull(std::string(get_value(argc, argv, "--remainder-bits", "12")))
    );
    options.universe_factor = static_cast<std::size_t>(
        std::stoull(std::string(get_value(argc, argv, "--universe-factor", "256")))
    );
    options.op_count = static_cast<std::size_t>(
        std::stoull(std::string(get_value(argc, argv, "--op-count", "40000")))
    );
    options.target_load =
        std::stod(std::string(get_value(argc, argv, "--target-load", "0.70")));
    options.query_mode = std::string(get_value(argc, argv, "--query-mode", "negative"));
    options.workload = std::string(get_value(argc, argv, "--workload", "read_heavy"));
    options.git_hash = std::string(get_value(argc, argv, "--git-hash", options.git_hash));
    options.compile_time_ms =
        std::stoi(std::string(get_value(argc, argv, "--compile-time-ms", "-1")));
    return options;
}

void emit_header() {
    std::cout
        << "language,structure,workload,dataset,n,param_tuple,run_id,throughput_ns_per_op,"
           "memory_bytes,fpr,compile_time_ms,git_hash,compiler_version,observed_hits,"
           "observed_checksum,avg_probe_distance,max_probe_distance,max_cluster_length,"
           "avg_cluster_length\n";
}

[[nodiscard]] std::string format_load(double value) {
    std::ostringstream out;
    out << std::fixed << std::setprecision(2) << value;
    return out.str();
}

void emit_row(
    std::string_view structure,
    std::string_view workload,
    std::string_view dataset,
    std::size_t n,
    std::string_view param_tuple,
    std::size_t run_id,
    double ns_per_op,
    std::size_t memory_bytes,
    double fpr,
    int compile_time_ms,
    std::string_view git_hash,
    const Telemetry& telemetry = {}
) {
    std::cout << "cpp," << structure << ',' << workload << ',' << dataset << ',' << n << ','
              << param_tuple << ',' << run_id << ',' << ns_per_op << ',' << memory_bytes << ','
              << fpr << ',' << compile_time_ms << ',' << git_hash << ','
              << dsw::compiler_version_string() << ',' << telemetry.observed_hits << ','
              << telemetry.observed_checksum << ',' << telemetry.avg_probe_distance << ','
              << telemetry.max_probe_distance << ',' << telemetry.max_cluster_length << ','
              << telemetry.avg_cluster_length << '\n';
}

[[nodiscard]] std::string blocked_bloom_params(
    const Options& options,
    std::size_t k_hashes
) {
    return "bits_per_key=" + std::to_string(options.bits_per_key) + ";k=" +
           std::to_string(k_hashes) + ";query_mode=" + options.query_mode;
}

[[nodiscard]] std::string quotient_filter_params(const Options& options) {
    return "q=" + std::to_string(options.q) + ";remainder_bits=" +
           std::to_string(options.remainder_bits) + ";target_load=" +
           format_load(options.target_load);
}

[[nodiscard]] std::string elias_fano_params(
    const Options& options,
    const dsw::EliasFano& encoded
) {
    return "universe_factor=" + std::to_string(options.universe_factor) + ";density=" +
           dsw::density_label(options.universe_factor) + ";lower_bits=" +
           std::to_string(encoded.lower_bits());
}

[[nodiscard]] Telemetry quotient_filter_telemetry(
    const dsw::QuotientFilter& filter,
    long long observed_hits
) {
    Telemetry telemetry;
    telemetry.observed_hits = observed_hits;
    telemetry.avg_probe_distance = filter.average_probe_distance();
    telemetry.max_probe_distance =
        static_cast<long long>(filter.max_probe_distance());
    telemetry.max_cluster_length =
        static_cast<long long>(filter.max_cluster_length());
    telemetry.avg_cluster_length = filter.average_cluster_length();
    return telemetry;
}

int run_blocked_bloom(const Options& options) {
    const std::size_t k_hashes = dsw::recommended_k_hashes(options.bits_per_key);
    const auto keys = dsw::default_positive_keys(options.n);
    const auto negatives = dsw::default_negative_queries(options.n);
    const auto mixed_queries = dsw::make_mixed_queries(keys, negatives);
    const auto& query_workload = options.query_mode == "mixed" ? mixed_queries : negatives;

    emit_header();
    repeat_warmup(options.warmup_runs, [&] {
        dsw::BlockedBloomFilter filter(options.n, options.bits_per_key, k_hashes);
        for (const auto key : keys) {
            filter.insert(key);
        }
        dsw::do_not_optimize(filter.memory_bytes());
    });
    for (std::size_t run_id = 0; run_id < options.runs; ++run_id) {
        std::size_t memory_bytes = 0U;
        const auto elapsed_ns = dsw::measure_ns([&] {
            dsw::BlockedBloomFilter filter(options.n, options.bits_per_key, k_hashes);
            for (const auto key : keys) {
                filter.insert(key);
            }
            memory_bytes = filter.memory_bytes();
            dsw::do_not_optimize(memory_bytes);
        });
        emit_row(
            "blocked_bloom",
            "build",
            "dense",
            options.n,
            blocked_bloom_params(options, k_hashes),
            run_id,
            static_cast<double>(elapsed_ns) / static_cast<double>(keys.size()),
            memory_bytes,
            0.0,
            options.compile_time_ms,
            options.git_hash
        );
    }

    const auto filter =
        dsw::BlockedBloomFilter::build(keys, options.bits_per_key, k_hashes);
    for (const auto key : keys) {
        if (!filter.contains(key)) {
            throw std::runtime_error("false negative during Blocked Bloom sanity pass");
        }
    }
    const double fpr = dsw::empirical_false_positive_rate(filter, negatives);
    repeat_warmup(options.warmup_runs, [&] {
        dsw::do_not_optimize(run_blocked_bloom_contains(filter, query_workload));
    });

    const std::string_view workload =
        options.query_mode == "mixed" ? "contains_mixed" : "contains_negative";
    for (std::size_t run_id = 0; run_id < options.runs; ++run_id) {
        std::size_t hits = 0U;
        const auto elapsed_ns = dsw::measure_ns([&] {
            hits = run_blocked_bloom_contains(filter, query_workload);
            dsw::do_not_optimize(hits);
        });
        emit_row(
            "blocked_bloom",
            workload,
            "dense",
            options.n,
            blocked_bloom_params(options, k_hashes),
            run_id,
            static_cast<double>(elapsed_ns) / static_cast<double>(query_workload.size()),
            filter.memory_bytes(),
            fpr,
            options.compile_time_ms,
            options.git_hash,
            Telemetry{static_cast<long long>(hits)}
        );
    }
    return EXIT_SUCCESS;
}

int run_quotient_filter(const Options& options) {
    const std::size_t capacity = 1ULL << options.q;
    const std::size_t target_count = std::max<std::size_t>(
        1U,
        static_cast<std::size_t>(static_cast<double>(capacity) * options.target_load)
    );
    const auto keys =
        dsw::default_positive_keys(target_count + options.op_count * 2U);
    const auto negatives =
        dsw::default_negative_queries(target_count + options.op_count * 2U);

    emit_header();
    repeat_warmup(options.warmup_runs, [&] {
        dsw::QuotientFilter warm_filter = dsw::QuotientFilter::create(
            static_cast<std::uint32_t>(options.q),
            static_cast<std::uint32_t>(options.remainder_bits)
        );
        for (std::size_t index = 0; index < target_count; ++index) {
            (void)warm_filter.insert(keys[index]);
        }
        dsw::do_not_optimize(warm_filter.memory_bytes());
    });
    for (std::size_t run_id = 0; run_id < options.runs; ++run_id) {
        std::size_t memory_bytes = 0U;
        dsw::QuotientFilter built = dsw::QuotientFilter::create(
            static_cast<std::uint32_t>(options.q),
            static_cast<std::uint32_t>(options.remainder_bits)
        );
        const auto elapsed_ns = dsw::measure_ns([&] {
            for (std::size_t index = 0; index < target_count; ++index) {
                (void)built.insert(keys[index]);
            }
            memory_bytes = built.memory_bytes();
        });
        emit_row(
            "quotient_filter",
            "build_insert",
            "dense",
            target_count,
            quotient_filter_params(options),
            run_id,
            static_cast<double>(elapsed_ns) / static_cast<double>(target_count),
            memory_bytes,
            0.0,
            options.compile_time_ms,
            options.git_hash,
            quotient_filter_telemetry(built, 0)
        );
    }

    dsw::QuotientFilter baseline = dsw::QuotientFilter::create(
        static_cast<std::uint32_t>(options.q),
        static_cast<std::uint32_t>(options.remainder_bits)
    );
    for (std::size_t index = 0; index < target_count; ++index) {
        (void)baseline.insert(keys[index]);
    }
    for (std::size_t index = 0; index < target_count; index += std::max<std::size_t>(1U, target_count / 64U)) {
        if (!baseline.contains(keys[index])) {
            throw std::runtime_error("Quotient Filter sanity check failed");
        }
    }
    std::size_t negative_hits = 0U;
    for (std::size_t index = 0; index < target_count; ++index) {
        negative_hits += static_cast<std::size_t>(baseline.contains(negatives[index]));
    }
    const double fpr = static_cast<double>(negative_hits) / static_cast<double>(target_count);

    if (options.workload == "read_heavy") {
        repeat_warmup(options.warmup_runs, [&] {
            dsw::do_not_optimize(
                run_quotient_read_heavy(baseline, keys, negatives, target_count, options.op_count)
            );
        });

        for (std::size_t run_id = 0; run_id < options.runs; ++run_id) {
            std::size_t hits = 0U;
            const auto elapsed_ns = dsw::measure_ns([&] {
                hits = run_quotient_read_heavy(
                    baseline, keys, negatives, target_count, options.op_count
                );
                dsw::do_not_optimize(hits);
            });
            emit_row(
                "quotient_filter",
                "read_heavy",
                "dense",
                target_count,
                quotient_filter_params(options),
                run_id,
                static_cast<double>(elapsed_ns) /
                    static_cast<double>(options.op_count * 2U),
                baseline.memory_bytes(),
                fpr,
                options.compile_time_ms,
                options.git_hash,
                quotient_filter_telemetry(baseline, static_cast<long long>(hits))
            );
        }
    } else if (options.workload == "mixed") {
        const std::size_t initial_count = std::max<std::size_t>(1U, target_count / 2U);
        repeat_warmup(options.warmup_runs, [&] {
            dsw::QuotientFilter warm_filter = dsw::QuotientFilter::create(
                static_cast<std::uint32_t>(options.q),
                static_cast<std::uint32_t>(options.remainder_bits)
            );
            for (std::size_t index = 0; index < initial_count; ++index) {
                (void)warm_filter.insert(keys[index]);
            }
            dsw::do_not_optimize(
                run_quotient_mixed(warm_filter, keys, negatives, initial_count, options.op_count)
            );
        });
        for (std::size_t run_id = 0; run_id < options.runs; ++run_id) {
            dsw::QuotientFilter filter = dsw::QuotientFilter::create(
                static_cast<std::uint32_t>(options.q),
                static_cast<std::uint32_t>(options.remainder_bits)
            );
            for (std::size_t index = 0; index < initial_count; ++index) {
                (void)filter.insert(keys[index]);
            }
            std::size_t hits = 0U;
            const auto elapsed_ns = dsw::measure_ns([&] {
                hits = run_quotient_mixed(
                    filter, keys, negatives, initial_count, options.op_count
                );
                dsw::do_not_optimize(hits);
            });
            emit_row(
                "quotient_filter",
                "mixed_ops",
                "dense",
                target_count,
                quotient_filter_params(options),
                run_id,
                static_cast<double>(elapsed_ns) / static_cast<double>(options.op_count),
                filter.memory_bytes(),
                fpr,
                options.compile_time_ms,
                options.git_hash,
                quotient_filter_telemetry(filter, static_cast<long long>(hits))
            );
        }
    } else if (options.workload == "delete_heavy") {
        repeat_warmup(options.warmup_runs, [&] {
            dsw::QuotientFilter warm_filter = baseline;
            for (std::size_t index = 0; index < target_count / 2U; ++index) {
                (void)warm_filter.erase(keys[index]);
            }
            dsw::do_not_optimize(warm_filter.memory_bytes());
        });
        repeat_warmup(options.warmup_runs, [&] {
            dsw::QuotientFilter warm_filter = baseline;
            for (std::size_t index = 0; index < target_count / 2U; ++index) {
                (void)warm_filter.erase(keys[index]);
            }
            dsw::do_not_optimize(
                run_quotient_delete_queries(warm_filter, keys, target_count)
            );
        });
        for (std::size_t run_id = 0; run_id < options.runs; ++run_id) {
            dsw::QuotientFilter filter = baseline;
            const auto erase_elapsed_ns = dsw::measure_ns([&] {
                for (std::size_t index = 0; index < target_count / 2U; ++index) {
                    (void)filter.erase(keys[index]);
                }
            });
            emit_row(
                "quotient_filter",
                "erase_delete_heavy",
                "dense",
                target_count,
                quotient_filter_params(options),
                run_id,
                static_cast<double>(erase_elapsed_ns) /
                    static_cast<double>(std::max<std::size_t>(1U, target_count / 2U)),
                filter.memory_bytes(),
                fpr,
                options.compile_time_ms,
                options.git_hash,
                quotient_filter_telemetry(filter, 0)
            );

            std::size_t hits = 0U;
            const auto query_elapsed_ns = dsw::measure_ns([&] {
                hits = run_quotient_delete_queries(filter, keys, target_count);
                dsw::do_not_optimize(hits);
            });
            emit_row(
                "quotient_filter",
                "contains_delete_heavy",
                "dense",
                target_count,
                quotient_filter_params(options),
                run_id,
                static_cast<double>(query_elapsed_ns) / static_cast<double>(target_count),
                filter.memory_bytes(),
                fpr,
                options.compile_time_ms,
                options.git_hash,
                quotient_filter_telemetry(filter, static_cast<long long>(hits))
            );
        }
    } else {
        throw std::runtime_error("unknown quotient_filter workload");
    }

    return EXIT_SUCCESS;
}

int run_elias_fano(const Options& options) {
    const auto values = dsw::make_sorted_unique_values(options.n, options.universe_factor);
    const std::string dataset = dsw::density_label(options.universe_factor);
    emit_header();

    repeat_warmup(options.warmup_runs, [&] {
        const auto warm_encoded = dsw::EliasFano::build(values, values.back());
        dsw::do_not_optimize(warm_encoded.memory_bytes());
    });
    for (std::size_t run_id = 0; run_id < options.runs; ++run_id) {
        std::size_t memory_bytes = 0U;
        dsw::EliasFano encoded;
        const auto elapsed_ns = dsw::measure_ns([&] {
            encoded = dsw::EliasFano::build(values, values.back());
            memory_bytes = encoded.memory_bytes();
        });
        emit_row(
            "elias_fano",
            "build",
            dataset,
            options.n,
            "universe_factor=" + std::to_string(options.universe_factor) + ";density=" + dataset +
                ";lower_bits=" + std::to_string(encoded.lower_bits()),
            run_id,
            static_cast<double>(elapsed_ns) / static_cast<double>(values.size()),
            memory_bytes,
            0.0,
            options.compile_time_ms,
            options.git_hash
        );
    }

    const auto encoded = dsw::EliasFano::build(values, values.back());
    for (std::size_t index = 0; index < values.size(); index += std::max<std::size_t>(1U, values.size() / 64U)) {
        if (encoded.select(index) != values[index]) {
            throw std::runtime_error("Elias-Fano select sanity check failed");
        }
    }

    if (options.workload == "contains") {
        repeat_warmup(options.warmup_runs, [&] {
            dsw::do_not_optimize(run_elias_fano_contains(encoded, values, options.op_count));
        });
        std::size_t hits = 0U;
        for (std::size_t run_id = 0; run_id < options.runs; ++run_id) {
            hits = 0U;
            const auto elapsed_ns = dsw::measure_ns([&] {
                hits = run_elias_fano_contains(encoded, values, options.op_count);
                dsw::do_not_optimize(hits);
            });
            emit_row(
                "elias_fano",
                "contains",
                dataset,
                options.n,
                elias_fano_params(options, encoded),
                run_id,
                static_cast<double>(elapsed_ns) / static_cast<double>(options.op_count),
                encoded.memory_bytes(),
                0.0,
                options.compile_time_ms,
                options.git_hash,
                Telemetry{static_cast<long long>(hits)}
            );
        }
    } else if (options.workload == "select") {
        repeat_warmup(options.warmup_runs, [&] {
            dsw::do_not_optimize_u64(
                run_elias_fano_select_checksum(encoded, values.size(), options.op_count)
            );
        });
        std::uint64_t checksum = 0U;
        for (std::size_t run_id = 0; run_id < options.runs; ++run_id) {
            checksum = 0U;
            const auto elapsed_ns = dsw::measure_ns([&] {
                checksum = run_elias_fano_select_checksum(
                    encoded, values.size(), options.op_count
                );
                dsw::do_not_optimize_u64(checksum);
            });
            emit_row(
                "elias_fano",
                "select",
                dataset,
                options.n,
                elias_fano_params(options, encoded),
                run_id,
                static_cast<double>(elapsed_ns) / static_cast<double>(options.op_count),
                encoded.memory_bytes(),
                0.0,
                options.compile_time_ms,
                options.git_hash,
                Telemetry{-1, checksum}
            );
        }
    } else if (options.workload == "predecessor") {
        repeat_warmup(options.warmup_runs, [&] {
            dsw::do_not_optimize(
                run_elias_fano_predecessor(encoded, values, options.op_count)
            );
        });
        std::size_t hits = 0U;
        for (std::size_t run_id = 0; run_id < options.runs; ++run_id) {
            hits = 0U;
            const auto elapsed_ns = dsw::measure_ns([&] {
                hits = run_elias_fano_predecessor(encoded, values, options.op_count);
                dsw::do_not_optimize(hits);
            });
            emit_row(
                "elias_fano",
                "predecessor",
                dataset,
                options.n,
                elias_fano_params(options, encoded),
                run_id,
                static_cast<double>(elapsed_ns) / static_cast<double>(options.op_count),
                encoded.memory_bytes(),
                0.0,
                options.compile_time_ms,
                options.git_hash,
                Telemetry{static_cast<long long>(hits)}
            );
        }
    } else {
        throw std::runtime_error("unknown elias_fano workload");
    }

    return EXIT_SUCCESS;
}

}  // namespace

int main(int argc, char** argv) {
    try {
        if (argc < 2) {
            std::cerr << "usage: cpp_bench <blocked_bloom|quotient_filter|elias_fano> [options]\n";
            return EXIT_FAILURE;
        }

        const std::string_view command = argv[1];
        const Options options = parse_options(argc, argv);

        if (command == "blocked_bloom") {
            return run_blocked_bloom(options);
        }
        if (command == "quotient_filter") {
            return run_quotient_filter(options);
        }
        if (command == "elias_fano") {
            return run_elias_fano(options);
        }

        std::cerr << "unknown subcommand: " << command << '\n';
        return EXIT_FAILURE;
    } catch (const std::exception& error) {
        std::cerr << "cpp_bench failure: " << error.what() << '\n';
        return EXIT_FAILURE;
    }
}
