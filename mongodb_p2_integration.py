from datetime import datetime, timezone
from pyspark.sql import SparkSession
from mongodb_client import get_database, client

def main():
    spark = (
        SparkSession.builder
        .appName("P2-MongoDB-Integration")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    db = get_database()
    quarantine_collection = db["quarantine_records"]
    recovery_collection = db["recovery_logs"]

    run_id = "P2_MONGODB_TEST"

    # Clear only records from this integration test.
    quarantine_collection.delete_many({"integration_run": run_id})
    recovery_collection.delete_many({"integration_run": run_id})

    input_df = spark.read.option("header", True).option(
        "inferSchema", False
    ).csv("outputs/p1/faulty_stop_times")

    quarantine_df = spark.read.parquet("outputs/p2/quarantine")
    recovered_df = spark.read.parquet("outputs/p2/recovered")

    input_count = input_df.count()
    quarantine_count = quarantine_df.count()
    recovered_count = recovered_df.count()

    valid_count = input_count - quarantine_count
    duplicates_removed = valid_count - recovered_count

    timestamp = datetime.now(timezone.utc)

    print("\nP2 QUARANTINE TO MONGODB")
    print("--------------------------------")

    documents = []

    for row in quarantine_df.toLocalIterator():
        record = row.asDict(recursive=True)
        reason = record.pop("quarantine_reason", "UNKNOWN")

        documents.append({
            "fault_type": reason,
            "record": record,
            "quarantined_at": timestamp,
            "integration_run": run_id
        })

        if len(documents) >= 1000:
            quarantine_collection.insert_many(documents)
            documents = []

    if documents:
        quarantine_collection.insert_many(documents)

    stored_quarantine = quarantine_collection.count_documents(
        {"integration_run": run_id}
    )

    recovery_summary = {
        "integration_run": run_id,
        "input_records": input_count,
        "quarantined_records": quarantine_count,
        "valid_records_before_deduplication": valid_count,
        "duplicates_removed": duplicates_removed,
        "recovered_records": recovered_count,
        "quarantine_records_stored": stored_quarantine,
        "recovery_status": "SUCCESS",
        "created_at": timestamp
    }

    recovery_collection.insert_one(recovery_summary)

    print(f"Input records: {input_count}")
    print(f"Quarantined records: {quarantine_count}")
    print(f"Recovered records: {recovered_count}")
    print(f"Quarantine records stored: {stored_quarantine}")

    print("\nP2 RECOVERY SUMMARY STORED")
    print("--------------------------------")
    print(f"Recovery logs: {recovery_collection.count_documents({'integration_run': run_id})}")
    print(f"Recovery status: SUCCESS")
    print(f"Database: {db.name}")

    spark.stop()
    client.close()

if __name__ == "__main__":
    main()
