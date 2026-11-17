
import json

from kafka import KafkaProducer, KafkaConsumer
from validation_framework import validate_record

KAFKA_BROKER = "localhost:9092"

INPUT_TOPIC = "gtfs-fault-test"
VALID_TOPIC = "gtfs-validated"
QUARANTINE_TOPIC = "gtfs-quarantine"

CONSUMER_GROUP = "gtfs-fault-test-consumer-v1"


def main():
    print("STARTING CONTROLLED KAFKA FAULT TEST")

    # 1. Create a sample GTFS record with an intentional fault
    faulty_record = {
        "trip_id": "FAULT_TEST_001",
        "arrival_time": "INVALID_TIME",
        "departure_time": "04:30:00",
        "stop_id": "4736",
        "stop_sequence": "1",
        "stop_headsign": "",
        "pickup_type": "0",
        "drop_off_type": "0",
        "shape_dist_traveled": "",
        "timepoint": "1"
    }

    print("\nOriginal test record:")
    print(json.dumps(faulty_record, indent=2))

    # 2. Publish the record to the dedicated test topic
    producer = KafkaProducer(
        bootstrap_servers=[KAFKA_BROKER],
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
        acks="all"
    )

    producer.send(
        INPUT_TOPIC,
        key=faulty_record["trip_id"],
        value=faulty_record
    ).get(timeout=30)

    producer.flush()
    producer.close()

    print(f"\nPublished test record to: {INPUT_TOPIC}")

    # 3. Consume the record from the test topic
    consumer = KafkaConsumer(
        bootstrap_servers=[KAFKA_BROKER],
        group_id=CONSUMER_GROUP,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda value: json.loads(value.decode("utf-8"))
    )

    consumer.subscribe([INPUT_TOPIC])

    try:
        message = consumer.poll(timeout_ms=15000)

        if not message:
            raise RuntimeError("No test record received from Kafka")

        for messages in message.values():
            for kafka_message in messages:
                record = kafka_message.value

                # 4. Validate using our existing validation framework
                result = validate_record(record)

                print("\nVALIDATION RESULT:")
                print(json.dumps(result, indent=2))

                # 5. Route the record based on validation result
                if result["is_valid"]:
                    destination = VALID_TOPIC
                    output_record = record
                else:
                    destination = QUARANTINE_TOPIC
                    output_record = {
                        "record": record,
                        "errors": result["errors"]
                    }

                producer_out = KafkaProducer(
                    bootstrap_servers=[KAFKA_BROKER],
                    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
                    key_serializer=lambda key: key.encode("utf-8"),
                    acks="all"
                )

                producer_out.send(
                    destination,
                    key=record["trip_id"],
                    value=output_record
                ).get(timeout=30)

                producer_out.flush()
                producer_out.close()

                print(f"\nRecord routed to: {destination}")

                if not result["is_valid"]:
                    print("CONTROLLED FAULT DETECTED SUCCESSFULLY")
                    print("QUARANTINE ROUTING SUCCESSFUL")

                else:
                    print("Record passed validation.")

    finally:
        consumer.close()


if __name__ == "__main__":
    main()
