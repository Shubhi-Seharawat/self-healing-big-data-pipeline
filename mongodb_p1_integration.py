from datetime import datetime, timezone
from pyspark.sql import SparkSession
from mongodb_client import get_database, client

REPORTS = {
    "missing_trip_id": "outputs/p1/missing_trip_id",
    "invalid_arrival_time": "outputs/p1/invalid_arrival_time",
    "duplicate_groups": "outputs/p1/duplicate_groups",
    "unexpected_pickup_type": "outputs/p1/unexpected_pickup_type",
    "invalid_stop_sequence": "outputs/p1/invalid_stop_sequence"
}

def main():
    spark = (
        SparkSession.builder
        .appName("P1-MongoDB-Integration")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    db = get_database()
    collection = db["detected_faults"]

    # Remove records from an earlier run of this integration test.
    collection.delete_many({
        "integration_run": "P1_MONGODB_TEST"
    })

    total_inserted = 0
    detected_at = datetime.now(timezone.utc)

    print("\nP1 FAULT DETECTION TO MONGODB")
    print("--------------------------------")

    for fault_type, path in REPORTS.items():
        report_df = spark.read.parquet(path)
        records = report_df.toLocalIterator()

        documents = []

        for row in records:
            document = {
                "fault_type": fault_type,
                "record": row.asDict(recursive=True),
                "detected_at": detected_at,
                "integration_run": "P1_MONGODB_TEST"
            }
            documents.append(document)

            if len(documents) >= 1000:
                collection.insert_many(documents)
                total_inserted += len(documents)
                documents = []

        if documents:
            collection.insert_many(documents)
            total_inserted += len(documents)

        count = collection.count_documents({
            "fault_type": fault_type,
            "integration_run": "P1_MONGODB_TEST"
        })

        print(f"{fault_type}: {count} records stored")

    print("--------------------------------")
    print(f"Total fault records stored: {total_inserted}")
    print(f"MongoDB collection: {collection.name}")
    print(f"Database: {db.name}")

    spark.stop()
    client.close()

if __name__ == "__main__":
    main()
