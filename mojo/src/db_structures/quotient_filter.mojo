from db_structures.hash import hash_uint64
from shared.hash_constants import QUOTIENT_FILTER_CLUSTER_RESERVE


struct Fingerprint(TrivialRegisterPassable, ImplicitlyCopyable, Copyable, Movable):
    var quotient: Int
    var remainder: UInt32

    def __init__(out self, quotient: Int, remainder: UInt32):
        self.quotient = quotient
        self.remainder = remainder


struct Entry(TrivialRegisterPassable, ImplicitlyCopyable, Copyable, Movable):
    var quotient: Int
    var remainder: UInt32

    def __init__(out self, quotient: Int, remainder: UInt32):
        self.quotient = quotient
        self.remainder = remainder


struct Instrumentation(TrivialRegisterPassable, ImplicitlyCopyable, Copyable, Movable):
    var insert_count: Int
    var total_probe_distance: Int
    var max_probe_distance: Int
    var total_cluster_length: Int
    var max_cluster_length: Int

    def __init__(out self):
        self.insert_count = 0
        self.total_probe_distance = 0
        self.max_probe_distance = 0
        self.total_cluster_length = 0
        self.max_cluster_length = 0


struct QuotientFilter(Copyable, Movable):
    var capacity_pow2: Int
    var remainder_bits: Int
    var capacity: Int
    var index_mask: Int
    var remainder_mask: UInt32
    var occupied_mask: UInt32
    var continuation_mask: UInt32
    var shifted_mask: UInt32
    var count: Int
    var slots: List[UInt32]
    var instrumentation_data: Instrumentation
    var scratch_items: List[Entry]
    var scratch_occupied_quotients: List[Int]
    var scratch_positions: List[Int]

    def __init__(out self, capacity_pow2: Int, remainder_bits: Int):
        self.capacity_pow2 = capacity_pow2
        self.remainder_bits = remainder_bits
        self.capacity = 1 << capacity_pow2
        self.index_mask = self.capacity - 1
        self.remainder_mask = (UInt32(1) << UInt32(remainder_bits)) - UInt32(1)
        self.occupied_mask = UInt32(1) << UInt32(remainder_bits)
        self.continuation_mask = UInt32(1) << UInt32(remainder_bits + 1)
        self.shifted_mask = UInt32(1) << UInt32(remainder_bits + 2)
        self.count = 0
        self.slots = List[UInt32](length=self.capacity, fill=UInt32(0))
        self.instrumentation_data = Instrumentation()
        self.scratch_items = List[Entry]()
        self.scratch_items.reserve(QUOTIENT_FILTER_CLUSTER_RESERVE)
        self.scratch_occupied_quotients = List[Int]()
        self.scratch_occupied_quotients.reserve(QUOTIENT_FILTER_CLUSTER_RESERVE)
        self.scratch_positions = List[Int]()
        self.scratch_positions.reserve(QUOTIENT_FILTER_CLUSTER_RESERVE)

    @staticmethod
    def create(capacity_pow2: Int, remainder_bits: Int) -> QuotientFilter:
        var filter = QuotientFilter(capacity_pow2, remainder_bits)
        return filter^

    def insert(mut self, key: UInt64) -> Bool:
        if self.count >= self.capacity:
            return False

        var fingerprint = self.split_fingerprint(key)
        if self.contains_fingerprint(fingerprint.quotient, fingerprint.remainder):
            return False

        if self.slot_empty(fingerprint.quotient):
            self.slots[fingerprint.quotient] = self.make_slot(
                fingerprint.remainder, True, False, False
            )
            self.count += 1
            self.record_insert_stats(0, 1)
            return True

        var cluster_start = self.find_cluster_start(fingerprint.quotient)
        self.materialize_cluster(cluster_start)
        var insert_position = self.find_insert_position(
            self.scratch_items, cluster_start, fingerprint.quotient, fingerprint.remainder
        )
        self.scratch_items.reserve(len(self.scratch_items) + 1)
        var inserted_entry = Entry(fingerprint.quotient, fingerprint.remainder)
        self.scratch_items.append(inserted_entry)
        var move_index = len(self.scratch_items) - 1
        while move_index > insert_position:
            self.scratch_items[move_index] = self.scratch_items[move_index - 1]
            move_index -= 1
        self.scratch_items[insert_position] = inserted_entry

        var cluster_length = len(self.scratch_items)
        self.rewrite_cluster(cluster_start, cluster_length)
        self.scratch_items.clear()
        self.scratch_occupied_quotients.clear()
        self.scratch_positions.clear()
        self.count += 1
        var physical_slot = self.advance(cluster_start, insert_position)
        self.record_insert_stats(self.distance(fingerprint.quotient, physical_slot), cluster_length)
        return True

    def contains(self, key: UInt64) -> Bool:
        var fingerprint = self.split_fingerprint(key)
        return self.contains_fingerprint(fingerprint.quotient, fingerprint.remainder)

    def erase(mut self, key: UInt64) -> Bool:
        var fingerprint = self.split_fingerprint(key)
        if not self.occupied(fingerprint.quotient):
            return False

        var cluster_start = self.find_cluster_start(fingerprint.quotient)
        self.materialize_cluster(cluster_start)
        var clear_span = len(self.scratch_items)
        var match_index = -1
        for index in range(len(self.scratch_items)):
            if (
                self.scratch_items[index].quotient == fingerprint.quotient
                and self.scratch_items[index].remainder == fingerprint.remainder
            ):
                match_index = index
                break

        if match_index < 0:
            self.scratch_items.clear()
            self.scratch_occupied_quotients.clear()
            self.scratch_positions.clear()
            return False

        for index in range(match_index, len(self.scratch_items) - 1):
            self.scratch_items[index] = self.scratch_items[index + 1]
        _ = self.scratch_items.pop(len(self.scratch_items) - 1)
        self.rewrite_cluster(cluster_start, clear_span)
        self.scratch_items.clear()
        self.scratch_occupied_quotients.clear()
        self.scratch_positions.clear()
        self.count -= 1
        return True

    def load_factor(self) -> Float64:
        if self.capacity == 0:
            return 0.0
        return Float64(self.count) / Float64(self.capacity)

    def memory_bytes(self) -> Int:
        return len(self.slots) * 4

    def average_probe_distance(self) -> Float64:
        if self.instrumentation_data.insert_count == 0:
            return 0.0
        return Float64(self.instrumentation_data.total_probe_distance) / Float64(
            self.instrumentation_data.insert_count
        )

    def average_cluster_length(self) -> Float64:
        if self.instrumentation_data.insert_count == 0:
            return 0.0
        return Float64(self.instrumentation_data.total_cluster_length) / Float64(
            self.instrumentation_data.insert_count
        )

    def max_probe_distance(self) -> Int:
        return self.instrumentation_data.max_probe_distance

    def max_cluster_length(self) -> Int:
        return self.instrumentation_data.max_cluster_length

    def raw_slots(self) -> List[UInt32]:
        var slots = List[UInt32]()
        slots.reserve(len(self.slots))
        for slot in self.slots:
            slots.append(slot)
        return slots^

    def instrumentation(self) -> Instrumentation:
        return self.instrumentation_data

    @always_inline
    def split_fingerprint(self, key: UInt64) -> Fingerprint:
        var fingerprint = hash_uint64(key)
        return Fingerprint(
            Int(fingerprint & UInt64(self.index_mask)),
            UInt32((fingerprint >> UInt64(self.capacity_pow2)) & UInt64(self.remainder_mask)),
        )

    @always_inline
    def advance(self, index: Int, steps: Int) -> Int:
        return (index + steps) & self.index_mask

    @always_inline
    def decrement(self, index: Int) -> Int:
        return (index - 1) & self.index_mask

    @always_inline
    def increment(self, index: Int) -> Int:
        return (index + 1) & self.index_mask

    @always_inline
    def distance(self, start: Int, finish: Int) -> Int:
        return (finish + self.capacity - start) & self.index_mask

    @always_inline
    def slot_empty(self, index: Int) -> Bool:
        return self.slots[index] == UInt32(0)

    @always_inline
    def occupied(self, index: Int) -> Bool:
        return (self.slots[index] & self.occupied_mask) != UInt32(0)

    @always_inline
    def continuation(self, index: Int) -> Bool:
        return (self.slots[index] & self.continuation_mask) != UInt32(0)

    @always_inline
    def shifted(self, index: Int) -> Bool:
        return (self.slots[index] & self.shifted_mask) != UInt32(0)

    @always_inline
    def remainder(self, index: Int) -> UInt32:
        return self.slots[index] & self.remainder_mask

    @always_inline
    def make_slot(
        self,
        remainder_value: UInt32,
        occupied_bit: Bool,
        continuation_bit: Bool,
        shifted_bit: Bool,
    ) -> UInt32:
        var slot = remainder_value
        if occupied_bit:
            slot |= self.occupied_mask
        if continuation_bit:
            slot |= self.continuation_mask
        if shifted_bit:
            slot |= self.shifted_mask
        return slot

    def next_occupied(self, quotient: Int) -> Int:
        var cursor = self.increment(quotient)
        while not self.occupied(cursor):
            cursor = self.increment(cursor)
        return cursor

    def find_cluster_start(self, index: Int) -> Int:
        var cursor = index
        while not self.slot_empty(self.decrement(cursor)):
            cursor = self.decrement(cursor)
        return cursor

    def find_run_start(self, quotient: Int) -> Int:
        var cluster_start = self.find_cluster_start(quotient)
        var run_start = cluster_start
        var run_quotient = cluster_start

        while run_quotient != quotient:
            run_start = self.increment(run_start)
            while (not self.slot_empty(run_start)) and self.continuation(run_start):
                run_start = self.increment(run_start)
            run_quotient = self.next_occupied(run_quotient)

        return run_start

    def contains_fingerprint(self, quotient: Int, remainder_value: UInt32) -> Bool:
        if not self.occupied(quotient):
            return False

        var cursor = self.find_run_start(quotient)
        while True:
            var current = self.remainder(cursor)
            if current == remainder_value:
                return True
            if current > remainder_value:
                return False

            var next = self.increment(cursor)
            if self.slot_empty(next) or (not self.continuation(next)):
                return False
            cursor = next

    def materialize_cluster(mut self, cluster_start: Int):
        self.scratch_items.clear()
        if self.slot_empty(cluster_start):
            return

        var cursor = cluster_start
        var current_quotient = cluster_start
        var first = True
        while not self.slot_empty(cursor):
            if (not first) and (not self.continuation(cursor)):
                current_quotient = self.next_occupied(current_quotient)
            self.scratch_items.append(Entry(current_quotient, self.remainder(cursor)))
            cursor = self.increment(cursor)
            first = False

    def find_insert_position(
        self,
        read items: List[Entry],
        cluster_start: Int,
        quotient: Int,
        remainder_value: UInt32,
    ) -> Int:
        var target_rank = self.distance(cluster_start, quotient)
        for index in range(len(items)):
            var item_rank = self.distance(cluster_start, items[index].quotient)
            if item_rank > target_rank:
                return index
            if item_rank == target_rank and items[index].remainder >= remainder_value:
                return index
        return len(items)

    def rewrite_cluster(mut self, clear_start: Int, clear_span: Int):
        for index in range(clear_span):
            self.slots[self.advance(clear_start, index)] = UInt32(0)
        if len(self.scratch_items) == 0:
            return

        self.scratch_occupied_quotients.clear()
        for entry in self.scratch_items:
            if (
                len(self.scratch_occupied_quotients) == 0
                or self.scratch_occupied_quotients[
                    len(self.scratch_occupied_quotients) - 1
                ] != entry.quotient
            ):
                self.scratch_occupied_quotients.append(entry.quotient)

        self.scratch_positions.clear()
        self.scratch_positions.reserve(len(self.scratch_items))
        for _ in range(len(self.scratch_items)):
            self.scratch_positions.append(0)
        var next_free_rank = 0
        for index in range(len(self.scratch_items)):
            var quotient_rank = self.distance(clear_start, self.scratch_items[index].quotient)
            var position_rank = quotient_rank if quotient_rank > next_free_rank else next_free_rank
            self.scratch_positions[index] = self.advance(clear_start, position_rank)
            next_free_rank = position_rank + 1

        var occupied_cursor = 0
        for index in range(len(self.scratch_items)):
            var position = self.scratch_positions[index]
            var occupied_bit = (
                occupied_cursor < len(self.scratch_occupied_quotients)
                and self.scratch_occupied_quotients[occupied_cursor] == position
            )
            if occupied_bit:
                occupied_cursor += 1
            var continuation_bit = (
                index > 0
                and self.scratch_items[index].quotient == self.scratch_items[index - 1].quotient
            )
            var shifted_bit = position != self.scratch_items[index].quotient
            self.slots[position] = self.make_slot(
                self.scratch_items[index].remainder, occupied_bit, continuation_bit, shifted_bit
            )

    def record_insert_stats(mut self, probe_distance: Int, cluster_length: Int):
        self.instrumentation_data.insert_count += 1
        self.instrumentation_data.total_probe_distance += probe_distance
        if probe_distance > self.instrumentation_data.max_probe_distance:
            self.instrumentation_data.max_probe_distance = probe_distance
        self.instrumentation_data.total_cluster_length += cluster_length
        if cluster_length > self.instrumentation_data.max_cluster_length:
            self.instrumentation_data.max_cluster_length = cluster_length


def create_quotient_filter(capacity_pow2: Int, remainder_bits: Int) -> QuotientFilter:
    return QuotientFilter.create(capacity_pow2, remainder_bits)
