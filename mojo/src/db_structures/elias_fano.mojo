from std.bit import bit_width, count_trailing_zeros, pop_count
from shared.hash_constants import WORD_BITS, WORD_MASK, WORD_SHIFT


struct EliasFano(Copyable, Movable):
    var count: Int
    var universe_max: UInt64
    var lower_bits: Int
    var lower_mask: UInt64
    var upper_bit_length: Int
    var lower_words_data: List[UInt64]
    var upper_words_data: List[UInt64]

    def __init__(out self):
        self.count = 0
        self.universe_max = UInt64(0)
        self.lower_bits = 0
        self.lower_mask = UInt64(0)
        self.upper_bit_length = 0
        self.lower_words_data = List[UInt64]()
        self.upper_words_data = List[UInt64]()

    @staticmethod
    def build(read values: List[UInt64], universe_max: UInt64) -> EliasFano:
        var encoded = EliasFano()
        encoded.count = len(values)
        encoded.universe_max = universe_max
        if encoded.count == 0:
            return encoded^

        var ratio = universe_max // UInt64(encoded.count)
        if ratio < UInt64(1):
            ratio = UInt64(1)

        var ratio_width = Int(bit_width(ratio))
        encoded.lower_bits = ratio_width - 1 if ratio_width > 0 else 0
        encoded.lower_mask = (
            UInt64(0)
            if encoded.lower_bits == 0
            else (UInt64(1) << UInt64(encoded.lower_bits)) - UInt64(1)
        )

        var lower_total_bits = encoded.count * encoded.lower_bits
        encoded.lower_words_data = List[UInt64](
            length=(lower_total_bits + WORD_MASK) // WORD_BITS, fill=0
        )

        var upper_bound = Int(universe_max >> UInt64(encoded.lower_bits)) + encoded.count
        encoded.upper_bit_length = upper_bound + 1
        encoded.upper_words_data = List[UInt64](
            length=(encoded.upper_bit_length + WORD_MASK) // WORD_BITS, fill=0
        )

        for index in range(len(values)):
            var value = values[index]
            encoded.write_lower(index, value & encoded.lower_mask)
            var upper = Int(value >> UInt64(encoded.lower_bits))
            var upper_position = upper + index
            encoded.upper_words_data[upper_position >> WORD_SHIFT] |= (
                UInt64(1) << UInt64(upper_position & WORD_MASK)
            )

        return encoded^

    def contains(self, value: UInt64) -> Bool:
        var candidate = self.predecessor(value)
        return candidate != None and candidate.value() == value

    def select(self, index: Int) -> UInt64:
        var remaining = index
        for word_index in range(len(self.upper_words_data)):
            var word = self.upper_words_data[word_index]
            var bit_count = Int(pop_count(word))
            if remaining >= bit_count:
                remaining -= bit_count
                continue

            while remaining > 0:
                word &= word - UInt64(1)
                remaining -= 1
            var bit_index = Int(count_trailing_zeros(word))
            var position = word_index * WORD_BITS + bit_index
            var upper = UInt64(position - index)
            return (upper << UInt64(self.lower_bits)) | self.read_lower(index)

        return UInt64(0)

    def predecessor(self, value: UInt64) -> Optional[UInt64]:
        if self.count == 0:
            return None

        var left = 0
        var right = self.count
        while left < right:
            var middle = (left + right) // 2
            if self.select(middle) <= value:
                left = middle + 1
            else:
                right = middle
        if left == 0:
            return None
        return self.select(left - 1)

    def memory_bytes(self) -> Int:
        return (len(self.lower_words_data) + len(self.upper_words_data)) * 8

    def lower_words(self) -> List[UInt64]:
        var words = List[UInt64]()
        words.reserve(len(self.lower_words_data))
        for word in self.lower_words_data:
            words.append(word)
        return words^

    def upper_words(self) -> List[UInt64]:
        var words = List[UInt64]()
        words.reserve(len(self.upper_words_data))
        for word in self.upper_words_data:
            words.append(word)
        return words^

    @always_inline
    def write_lower(mut self, index: Int, lower_value: UInt64):
        if self.lower_bits == 0:
            return

        var offset = index * self.lower_bits
        var word_index = offset >> WORD_SHIFT
        var bit_offset = offset & WORD_MASK
        self.lower_words_data[word_index] |= lower_value << UInt64(bit_offset)
        if bit_offset + self.lower_bits > WORD_BITS:
            self.lower_words_data[word_index + 1] |= lower_value >> UInt64(WORD_BITS - bit_offset)

    @always_inline
    def read_lower(self, index: Int) -> UInt64:
        if self.lower_bits == 0:
            return UInt64(0)

        var offset = index * self.lower_bits
        var word_index = offset >> WORD_SHIFT
        var bit_offset = offset & WORD_MASK
        var value = self.lower_words_data[word_index] >> UInt64(bit_offset)
        if bit_offset + self.lower_bits > WORD_BITS:
            value |= self.lower_words_data[word_index + 1] << UInt64(WORD_BITS - bit_offset)
        return value & self.lower_mask
