
import json

from kafka import KafkaProducer
from pyspark.sql import SparkSession

from validation_framework import validate_record

KAFKA_BROKER = "localhost:9092"

INPUT_TOPIC = "gtfs-spark-test"
VALID_TOPIC = "gtfs-spark-validated"
QUARANTINE_TOPIC = "gtfs-spark-quarantine"

CHECKPOINT_LOCATION = "/tmp/data226_spark_kafka_checkpoint"

# Initialize Spark
spark = (
    SparkSession.builder
    .appName("DATA226-Self-Healing-Streaming")
    .master("local[*]")
    .config(
        "spark.jars.packages",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.4"
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

print("Spark Structured Streaming initialized.")
print("Input topic:", INPUT_TOPIC)


# Create Kafka producer for output routing
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    key_serializer=lambda key: key.encode("utf-8") if key else None,
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
    acks="all",
    retries=5
)


# Process each Spark micro-batch
def process_batch(batch_df, batch_id):

    valid_count = 0
    invalid_count = 0

    print(f"\nProcessing Spark batch: {batch_id}")

    records = batch_df.select("value").toLocalIterator()

    for row in records:

        record = json.loads(row["value"].decode("utf-8"))

        # Reuse the existing GTFS validation framework
        result = validate_record(record)

        if result["is_valid"]:

            producer.send(
                VALID_TOPIC,
                key=record.get("trip_id"),
                value=record
            )

            valid_count += 1

        else:

            quarantine_record = {
                "record": record,
                "errors": result["errors"]
            }

            producer.send(
                QUARANTINE_TOPIC,
                key=record.get("trip_id"),
                value=quarantine_record
            )

            invalid_count += 1

            print(
                f"Invalid record detected: "
                f"{record.get('trip_id')}"
            )

    # Ensure all output messages are delivered
    producer.flush()

    print(f"Valid records: {valid_count}")
    print(f"Invalid records: {invalid_count}")

    print(f"Batch {batch_id} completed.")


# Read continuously from Kafka
kafka_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)


# Process records using Spark Structured Streaming
query = (
    kafka_stream.writeStream
    .foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT_LOCATION)
    .trigger(availableNow=True)
    .start()
)

query.awaitTermination()

producer.close()
spark.stop()

print("\nSPARK SELF-HEALING STREAMING COMPLETED")
