
import csv
import json
from pathlib import Path

from kafka import KafkaProducer
from kafka.errors import KafkaError

# Project configuration
DATA_FILE = Path("data/gtfs/stop_times.txt")
KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "gtfs-raw"

BATCH_SIZE = 1000


def create_producer():
    """Create a Kafka producer with JSON serialization."""
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8") if key else None,
        acks="all",
        retries=5,
        max_in_flight_requests_per_connection=1,
        linger_ms=10,
        batch_size=32768
    )


def publish_gtfs_records():
    if not DATA_FILE.exists():
        raise FileNotFoundError(f"GTFS file not found: {DATA_FILE}")

    producer = create_producer()
    total_sent = 0

    print("Starting GTFS Kafka producer...")
    print(f"Source file: {DATA_FILE}")
    print(f"Kafka topic: {KAFKA_TOPIC}")

    try:
        with DATA_FILE.open("r", encoding="utf-8-sig", newline="") as file:
            reader = csv.DictReader(file)

            for record in reader:
                # Use trip_id as the Kafka message key
                message_key = record.get("trip_id") or None

                future = producer.send(
                    KAFKA_TOPIC,
                    key=message_key,
                    value=record
                )

                # Check broker acknowledgement
                future.get(timeout=30)
                total_sent += 1

                if total_sent % BATCH_SIZE == 0:
                    producer.flush()
                    print(f"Published {total_sent:,} records")

        producer.flush()

        print("\nKAFKA PRODUCER COMPLETED")
        print(f"Total records published: {total_sent:,}")
        print(f"Destination topic: {KAFKA_TOPIC}")

    except Exception as error:
        print(f"Producer error: {error}")
        raise

    finally:
        producer.close()


if __name__ == "__main__":
    publish_gtfs_records()

