#include <cstdlib>
#include <iostream>
#include <string_view>

#include "hash.hpp"

namespace {

void print_usage() {
    std::cout << "usage: cpp_hash <key> [seed]\n";
    std::cout << "       cpp_hash hash <key> [seed]\n";
    std::cout << "       cpp_hash dataset_key <index>\n";
    std::cout << "       cpp_hash negative_query <index>\n";
    std::cout << "       cpp_hash recommended_k_hashes <bits_per_key>\n";
}

}  // namespace

int main(int argc, char** argv) {
    if (argc < 2) {
        print_usage();
        return EXIT_SUCCESS;
    }

    const std::string_view command(argv[1]);
    if (command == "hash") {
        if (argc < 3) {
            print_usage();
            return EXIT_SUCCESS;
        }
        const auto key = static_cast<std::uint64_t>(std::stoull(argv[2]));
        const auto seed =
            argc >= 4 ? static_cast<std::uint64_t>(std::stoull(argv[3])) : 0ULL;
        std::cout << dsw::hash_uint64(key, seed) << '\n';
        return EXIT_SUCCESS;
    }

    if (command == "dataset_key") {
        if (argc < 3) {
            print_usage();
            return EXIT_SUCCESS;
        }
        const auto index = static_cast<std::uint64_t>(std::stoull(argv[2]));
        std::cout << dsw::dataset_key(index) << '\n';
        return EXIT_SUCCESS;
    }

    if (command == "negative_query") {
        if (argc < 3) {
            print_usage();
            return EXIT_SUCCESS;
        }
        const auto index = static_cast<std::uint64_t>(std::stoull(argv[2]));
        std::cout << dsw::negative_query(index) << '\n';
        return EXIT_SUCCESS;
    }

    if (command == "recommended_k_hashes") {
        if (argc < 3) {
            print_usage();
            return EXIT_SUCCESS;
        }
        const auto bits_per_key = static_cast<std::size_t>(std::stoull(argv[2]));
        std::cout << dsw::recommended_k_hashes(bits_per_key) << '\n';
        return EXIT_SUCCESS;
    }

    const auto key = static_cast<std::uint64_t>(std::stoull(argv[1]));
    const auto seed =
        argc >= 3 ? static_cast<std::uint64_t>(std::stoull(argv[2])) : 0ULL;
    std::cout << dsw::hash_uint64(key, seed) << '\n';
    return EXIT_SUCCESS;
}
