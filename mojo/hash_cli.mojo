from std.sys import argv

from db_structures.hash import dataset_key, hash_uint64, negative_query, recommended_k_hashes


def print_usage():
    print("usage: mojo_hash <key> [seed]")
    print("       mojo_hash hash <key> [seed]")
    print("       mojo_hash dataset_key <index>")
    print("       mojo_hash negative_query <index>")
    print("       mojo_hash recommended_k_hashes <bits_per_key>")


def main() raises:
    var args = argv()
    if len(args) < 2:
        print_usage()
        return

    if args[1] == "hash":
        if len(args) < 3:
            print_usage()
            return
        var key = UInt64(atol(args[2]))
        var seed = UInt64(0)
        if len(args) >= 4:
            seed = UInt64(atol(args[3]))
        print(hash_uint64(key, seed))
        return

    if args[1] == "dataset_key":
        if len(args) < 3:
            print_usage()
            return
        print(dataset_key(UInt64(atol(args[2]))))
        return

    if args[1] == "negative_query":
        if len(args) < 3:
            print_usage()
            return
        print(negative_query(UInt64(atol(args[2]))))
        return

    if args[1] == "recommended_k_hashes":
        if len(args) < 3:
            print_usage()
            return
        print(recommended_k_hashes(Int(atol(args[2]))))
        return

    var key = UInt64(atol(args[1]))
    var seed = UInt64(0)
    if len(args) >= 3:
        seed = UInt64(atol(args[2]))
    print(hash_uint64(key, seed))
