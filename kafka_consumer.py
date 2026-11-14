
import json

from kafka import KafkaConsumer, KafkaProducer
from jsonschema import Draft202012Validator

from validation_framework import load_schema

# Kafka configuration
KAFKA_BROKER = "localhost:9092"

INPUT_TOPIC = "gtfs-raw"
VALID_TOPIC = "gtfs-validated"
QUARANTINE_TOPIC = "gtfs-quarantine"

CONSUMER_GROUP = "gtfs-validation-consumer-v1"

EXPECTED_RECORDS = 387798
BATCH_SIZE = 500


def create_consumer():
    return KafkaConsumer(
        INPUT_TOPIC,
        bootstrap_servers=[KAFKA_BROKER],
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(
            value.decode("utf-8")
        ),
        max_poll_records=BATCH_SIZE
    )


def create_producer():
    return KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        key_serializer=lambda key: key.encode("utf-8") if key else None,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        acks="all",
        retries=5
    )


def process_records():

    schema = load_schema()
    validator = Draft202012Validator(schema)

    consumer = create_consumer()
    producer = create_producer()

    processed = 0
    valid_count = 0
    invalid_count = 0

    print("Starting Kafka consumer...")
    print(f"Input topic: {INPUT_TOPIC}")
    print(f"Expected records: {EXPECTED_RECORDS:,}")

    try:
        while processed < EXPECTED_RECORDS:

            remaining = EXPECTED_RECORDS - processed

            batches = consumer.poll(
                timeout_ms=1000,
                max_records=min(BATCH_SIZE, remaining)
            )

            if not batches:
                print(
                    f"Waiting for messages... "
                    f"Processed {processed:,}/{EXPECTED_RECORDS:,}"
                )
                continue

            for messages in batches.values():

                for message in messages:

                    record = message.value

                    errors = sorted(
                        validator.iter_errors(record),
                        key=lambda error: list(error.path)
                    )

                    if not errors:
                        producer.send(
                            VALID_TOPIC,
                            key=record.get("trip_id"),
                            value=record
                        )
                        valid_count += 1

                    else:
                        error_details = [
                            {
                                "field": ".".join(
                                    str(part) for part in error.path
                                ),
                                "message": error.message
                            }
                            for error in errors
                        ]

                        producer.send(
                            QUARANTINE_TOPIC,
                            key=record.get("trip_id"),
                            value={
                                "record": record,
                                "errors": error_details
                            }
                        )
                        invalid_count += 1

                    processed += 1

            # Confirm output messages before committing input offsets
            producer.flush()
            consumer.commit()

            if processed % 10000 < BATCH_SIZE:
                print(
                    f"Processed: {processed:,} | "
                    f"Valid: {valid_count:,} | "
                    f"Invalid: {invalid_count:,}"
                )

        print("\nKAFKA CONSUMER COMPLETED")
        print(f"Total processed: {processed:,}")
        print(f"Valid records: {valid_count:,}")
        print(f"Invalid records: {invalid_count:,}")

    finally:
        consumer.close()
        producer.close()


if __name__ == "__main__":
    process_records()
