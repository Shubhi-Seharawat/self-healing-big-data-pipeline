
import csv
import json
from pathlib import Path

from confluent_kafka import Producer, Consumer, TopicPartition
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import (
    JSONSerializer,
    JSONDeserializer,
)

# Configuration
KAFKA_BROKER = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"

SCHEMA_FILE = Path("schemas/gtfs_stop_times.schema.json")
DATA_FILE = Path("data/gtfs/stop_times.txt")

SCHEMA_SUBJECT = "gtfs-stop-times-value"
TEST_TOPIC = "gtfs-schema-test"


def subject_strategy(ctx, record_name):
    """Use the existing Schema Registry subject."""
    return SCHEMA_SUBJECT


def main():
    print("Starting Schema Registry + Kafka integration test...")

    # 1. Load the JSON Schema
    schema_str = SCHEMA_FILE.read_text(encoding="utf-8")

    # 2. Connect to Schema Registry
    registry_client = SchemaRegistryClient(
        {"url": SCHEMA_REGISTRY_URL}
    )

    # 3. Retrieve the latest registered schema
    registered = registry_client.get_latest_version(SCHEMA_SUBJECT)

    print(f"Registered subject: {registered.subject}")
    print(f"Schema version: {registered.version}")
    print(f"Schema ID: {registered.schema_id}")

    # 4. Create a Schema Registry JSON serializer
    serializer = JSONSerializer(
        schema_str,
        registry_client,
        to_dict=lambda obj, ctx: obj,
        conf={
            "auto.register.schemas": False,
            "use.latest.version": True,
            "subject.name.strategy": subject_strategy,
            "validate": True,
        },
    )

    # 5. Read one real GTFS record
    with DATA_FILE.open("r", encoding="utf-8", newline="") as file:
        reader = csv.DictReader(file)
        record = next(reader)

    
    print("\nOriginal GTFS record:")
    print(json.dumps(record, indent=2))

    # 6. Serialize the record using the registered schema
    context = SerializationContext(TEST_TOPIC, MessageField.VALUE)
    serialized_value = serializer(record, context)

    # 7. Publish the schema-serialized message to Kafka
    producer = Producer({"bootstrap.servers": KAFKA_BROKER})

    producer.produce(
        topic=TEST_TOPIC,
        key=record.get("trip_id"),
        value=serialized_value,
    )

    remaining = producer.flush(timeout=30)

    if remaining:
        raise RuntimeError(
            f"Kafka producer could not deliver {remaining} message(s)"
        )

    print(f"\nMessage successfully published to: {TEST_TOPIC}")

    # 8. Create a JSON Schema deserializer
    deserializer = JSONDeserializer(
        schema_str,
        from_dict=lambda obj, ctx: obj,
        schema_registry_client=registry_client,
    )

    # 9. Read the message from the beginning of the test topic
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": "schema-registry-integration-test-v1",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })

    try:
        consumer.assign([
            TopicPartition(TEST_TOPIC, 0, 0)
        ])

        message = consumer.poll(timeout=15)

        if message is None:
            raise RuntimeError("No Kafka message received within 15 seconds")

        if message.error():
            raise RuntimeError(f"Kafka consumer error: {message.error()}")

        # Deserialize the Schema Registry-framed message
        received_record = deserializer(
            message.value(),
            SerializationContext(TEST_TOPIC, MessageField.VALUE),
        )

        print("\nDeserialized Kafka record:")
        print(json.dumps(received_record, indent=2))

        # 10. Verify the received record
        if received_record != record:
            raise AssertionError(
                "The received record does not match the original record"
            )

        print("\nSCHEMA REGISTRY + KAFKA INTEGRATION SUCCESSFUL")
        print(f"Topic: {TEST_TOPIC}")
        print(f"Schema subject: {SCHEMA_SUBJECT}")
        print(f"Schema version: {registered.version}")
        print("Serialization: Successful")
        print("Deserialization: Successful")
        print("Record comparison: Passed")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
