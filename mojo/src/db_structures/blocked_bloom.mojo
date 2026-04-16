from db_structures.hash import BLOOM_BLOCK_SEED, BLOOM_STEP_SEED, BLOOM_INDEX_SEED, hash_uint64
from shared.hash_constants import BLOCKED_BLOOM_BLOCK_BITS, BLOCKED_BLOOM_WORDS_PER_BLOCK, WORD_BITS, WORD_MASK, WORD_SHIFT

comptime BLOCK_BITS = BLOCKED_BLOOM_BLOCK_BITS
comptime WORDS_PER_BLOCK = BLOCKED_BLOOM_WORDS_PER_BLOCK


struct BlockedBloomFilter(Copyable, Movable):
    var num_blocks: Int
    var bits_per_key: Int
    var k_hashes: Int
    var block_words: List[UInt64]

    def __init__(out self, expected_keys: Int, bits_per_key: Int, k_hashes: Int):
        var total_bits = expected_keys * bits_per_key
        var block_count = (total_bits + (BLOCK_BITS - 1)) // BLOCK_BITS
        if block_count < 1:
            block_count = 1

        self.num_blocks = block_count
        self.bits_per_key = bits_per_key
        self.k_hashes = k_hashes if k_hashes > 0 else 1
        self.block_words = List[UInt64](length=block_count * WORDS_PER_BLOCK, fill=UInt64(0))

    @staticmethod
    def build(
        read keys: List[UInt64], bits_per_key: Int, k_hashes: Int
    ) -> BlockedBloomFilter:
        var filter = BlockedBloomFilter(len(keys), bits_per_key, k_hashes)
        for key in keys:
            filter.insert(key)
        return filter^

    def insert(mut self, key: UInt64):
        var base_word = self.block_base(key)
        var h1 = hash_uint64(key, UInt64(BLOOM_BLOCK_SEED))
        var h2 = hash_uint64(key, UInt64(BLOOM_STEP_SEED)) | 1

        for hash_index in range(self.k_hashes):
            var bit_position = self.probe_position(h1, h2, hash_index)
            var word_index = base_word + (bit_position >> WORD_SHIFT)
            var bit_mask = UInt64(1) << UInt64(bit_position & WORD_MASK)
            self.block_words[word_index] |= bit_mask

    def contains(self, key: UInt64) -> Bool:
        var base_word = self.block_base(key)
        var h1 = hash_uint64(key, UInt64(BLOOM_BLOCK_SEED))
        var h2 = hash_uint64(key, UInt64(BLOOM_STEP_SEED)) | 1

        for hash_index in range(self.k_hashes):
            var bit_position = self.probe_position(h1, h2, hash_index)
            var word_index = base_word + (bit_position >> WORD_SHIFT)
            var bit_mask = UInt64(1) << UInt64(bit_position & WORD_MASK)
            if (self.block_words[word_index] & bit_mask) == 0:
                return False
        return True

    def memory_bytes(self) -> Int:
        return len(self.block_words) * 8

    def debug_block_words(self) -> List[UInt64]:
        var words = List[UInt64]()
        words.reserve(len(self.block_words))
        for word in self.block_words:
            words.append(word)
        return words^

    @always_inline
    def block_index(self, key: UInt64) -> Int:
        var index_hash = hash_uint64(key, UInt64(BLOOM_INDEX_SEED))
        return Int(index_hash % UInt64(self.num_blocks))

    @always_inline
    def block_base(self, key: UInt64) -> Int:
        return self.block_index(key) * WORDS_PER_BLOCK

    @always_inline
    def probe_position(self, h1: UInt64, h2: UInt64, hash_index: Int) -> Int:
        return Int((h1 + UInt64(hash_index) * h2) & UInt64(BLOCK_BITS - 1))


def build_blocked_bloom(
    read keys: List[UInt64], bits_per_key: Int, k_hashes: Int
) -> BlockedBloomFilter:
    return BlockedBloomFilter.build(keys, bits_per_key, k_hashes)


def empirical_false_positive_rate(
    read filter: BlockedBloomFilter, read negatives: List[UInt64]
) -> Float64:
    if len(negatives) == 0:
        return 0.0

    var false_positives = 0
    for key in negatives:
        if filter.contains(key):
            false_positives += 1
    return Float64(false_positives) / Float64(len(negatives))
