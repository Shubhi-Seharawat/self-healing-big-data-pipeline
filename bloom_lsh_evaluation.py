import argparse
import csv
import hashlib
import random
from collections import defaultdict


class BloomFilter:
    def __init__(self, size=50000, hash_count=5):
        self.size = size
        self.hash_count = hash_count
        self.bits = bytearray((size + 7) // 8)

    def _hash_indexes(self, value):
        for i in range(self.hash_count):
            text = f"{i}:{value}".encode("utf-8")
            digest = hashlib.sha256(text).hexdigest()
            yield int(digest, 16) % self.size

    def add(self, value):
        for index in self._hash_indexes(value):
            byte_index = index // 8
            bit_index = index % 8
            self.bits[byte_index] |= 1 << bit_index

    def might_contain(self, value):
        for index in self._hash_indexes(value):
            byte_index = index // 8
            bit_index = index % 8

            if not (self.bits[byte_index] & (1 << bit_index)):
                return False

        return True


def load_records(file_path, limit=5000):
    records = []

    with open(file_path, "r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)

        for row in reader:
            records.append(row)

            if len(records) >= limit:
                break

    return records


def record_key(record):
    return "|".join(
        [
            record.get("trip_id", ""),
            record.get("arrival_time", ""),
            record.get("departure_time", ""),
            record.get("stop_id", ""),
            record.get("stop_sequence", ""),
        ]
    )


def run_bloom_filter_experiment(records):
    print("\n" + "=" * 60)
    print("BLOOM FILTER EXPERIMENT")
    print("=" * 60)

    inserted_items = [record_key(record) for record in records]

    fake_items = [
        f"fake_trip_{i}|fake_time|fake_departure|fake_stop|{i}"
        for i in range(len(records))
    ]

    configurations = [
        (10000, 3),
        (25000, 3),
        (50000, 5),
        (100000, 7),
    ]

    for filter_size, hash_count in configurations:
        bloom = BloomFilter(
            size=filter_size,
            hash_count=hash_count
        )

        for item in inserted_items:
            bloom.add(item)

        true_positives = sum(
            bloom.might_contain(item)
            for item in inserted_items
        )

        false_positives = sum(
            bloom.might_contain(item)
            for item in fake_items
        )

        true_negatives = len(fake_items) - false_positives

        false_positive_rate = (
            false_positives / len(fake_items)
            if fake_items
            else 0
        )

        print(f"\nFilter size: {filter_size}")
        print(f"Hash functions: {hash_count}")
        print(f"Inserted items: {len(inserted_items)}")
        print(f"Positive queries detected: {true_positives}")
        print(f"True negatives: {true_negatives}")
        print(f"False positives: {false_positives}")
        print(f"False-positive rate: {false_positive_rate:.4%}")


def token_set(record):
    tokens = set()

    for value in record.values():
        if value is not None:
            value = str(value).strip().lower()

            if value:
                tokens.add(value)

    return tokens


def jaccard_similarity(first_set, second_set):
    union = first_set | second_set

    if not union:
        return 1.0

    return len(first_set & second_set) / len(union)


class SimpleLSH:
    def __init__(self, bands=16, rows_per_band=2):
        self.bands = bands
        self.rows_per_band = rows_per_band
        self.num_hashes = bands * rows_per_band
        self.buckets = defaultdict(set)

    def _hash_token(self, seed, token):
        value = f"{seed}:{token}".encode("utf-8")
        return int(hashlib.md5(value).hexdigest(), 16)

    def signature(self, tokens):
        signature = []

        for seed in range(self.num_hashes):
            if not tokens:
                signature.append(0)
                continue

            minimum_value = min(
                self._hash_token(seed, token)
                for token in tokens
            )

            signature.append(minimum_value)

        return signature

    def add(self, record_id, tokens):
        signature = self.signature(tokens)

        for band in range(self.bands):
            start = band * self.rows_per_band
            end = start + self.rows_per_band
            band_values = tuple(signature[start:end])

            bucket_key = (band, band_values)
            self.buckets[bucket_key].add(record_id)

    def candidates(self, tokens):
        signature = self.signature(tokens)
        candidate_ids = set()

        for band in range(self.bands):
            start = band * self.rows_per_band
            end = start + self.rows_per_band
            band_values = tuple(signature[start:end])

            bucket_key = (band, band_values)
            candidate_ids.update(self.buckets.get(bucket_key, set()))

        return candidate_ids


def run_lsh_experiment(records):
    print("\n" + "=" * 60)
    print("LOCALITY-SENSITIVE HASHING EXPERIMENT")
    print("=" * 60)

    sample_records = records[:100]

    if len(sample_records) < 2:
        print("Not enough records for the LSH experiment.")
        return

    lsh = SimpleLSH(bands=16, rows_per_band=2)

    token_sets = {}

    for record_id, record in enumerate(sample_records):
        tokens = token_set(record)
        token_sets[record_id] = tokens
        lsh.add(record_id, tokens)

    original_record = sample_records[0].copy()

    modified_record = original_record.copy()
    modified_record["arrival_time"] = "modified_time"

    modified_tokens = token_set(modified_record)
    candidate_ids = lsh.candidates(modified_tokens)

    print(f"Indexed records: {len(sample_records)}")
    print(f"Candidate record IDs: {sorted(candidate_ids)}")

    similarities = []

    for candidate_id in sorted(candidate_ids):
        similarity = jaccard_similarity(
            modified_tokens,
            token_sets[candidate_id]
        )

        similarities.append((candidate_id, similarity))

    similarities.sort(
        key=lambda item: item[1],
        reverse=True
    )

    print("\nCandidate similarities:")

    for candidate_id, similarity in similarities:
        print(
            f"Record {candidate_id}: "
            f"{similarity:.4f} Jaccard similarity"
        )

    exact_comparisons = len(sample_records)
    lsh_comparisons = len(candidate_ids)

    print("\nComparison summary:")
    print(f"Exact-search comparisons: {exact_comparisons}")
    print(f"LSH candidate comparisons: {lsh_comparisons}")

    if exact_comparisons > 0:
        reduction = 1 - (
            lsh_comparisons / exact_comparisons
        )

        print(
            f"Comparison reduction: {reduction:.2%}"
        )


def main():
    parser = argparse.ArgumentParser(
        description="Bloom Filter and LSH evaluation"
    )

    parser.add_argument(
        "--data",
        required=True,
        help="Path to stop_times.txt"
    )

    parser.add_argument(
        "--limit",
        type=int,
        default=5000,
        help="Number of records to process"
    )

    args = parser.parse_args()

    print("Loading GTFS records...")
    records = load_records(
        args.data,
        limit=args.limit
    )

    print(f"Records loaded: {len(records)}")

    if not records:
        raise ValueError(
            "No records were loaded. Check the file path."
        )

    run_bloom_filter_experiment(records)
    run_lsh_experiment(records)


if __name__ == "__main__":
    main()