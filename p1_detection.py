from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

spark = (
    SparkSession.builder
    .appName("P1-Five-Fault-Detection")
    .master("local[*]")
    .getOrCreate()
)

# Read the controlled faulty dataset
input_path = "outputs/p1/faulty_stop_times"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .option("mode", "FAILFAST")
    .csv(input_path)
)

# Fault 1: Missing required trip_id
missing_trip_id = df.filter(
    col("trip_id").isNull() | (col("trip_id") == "")
)

# Fault 2: Invalid arrival_time format
invalid_arrival_time = df.filter(
    col("arrival_time").isNull()
    | ~col("arrival_time").rlike(r"^\d{2}:\d{2}:\d{2}$")
)

# Fault 3: Duplicate records
duplicate_groups = (
    df.groupBy(*df.columns)
    .agg(count("*").alias("record_count"))
    .filter(col("record_count") > 1)
)

# Fault 4: Unexpected pickup_type (valid GTFS values: 0, 1, 2, 3)
unexpected_pickup_type = df.filter(
    col("pickup_type").isNull()
    | ~col("pickup_type").isin("0", "1", "2", "3")
)

# Fault 5: Invalid stop_sequence (must be a positive integer)
invalid_stop_sequence = df.filter(
    col("stop_sequence").isNull()
    | ~col("stop_sequence").rlike(r"^[1-9]\d*$")
)

# Display detection results
print("\n========== P1 FIVE-FAULT DETECTION RESULTS ==========")
print(f"Total records processed: {df.count()}")
print(f"1. Missing trip_id: {missing_trip_id.count()}")
print(f"2. Invalid arrival_time: {invalid_arrival_time.count()}")
print(f"3. Duplicate groups: {duplicate_groups.count()}")
print(f"4. Unexpected pickup_type: {unexpected_pickup_type.count()}")
print(f"5. Invalid stop_sequence: {invalid_stop_sequence.count()}")

# Save individual detection reports
reports = {
    "missing_trip_id": missing_trip_id,
    "invalid_arrival_time": invalid_arrival_time,
    "duplicate_groups": duplicate_groups,
    "unexpected_pickup_type": unexpected_pickup_type,
    "invalid_stop_sequence": invalid_stop_sequence
}

for report_name, report_df in reports.items():
    report_df.write.mode("overwrite").parquet(
        f"outputs/p1/{report_name}"
    )

print("\nAll five detection reports saved under outputs/p1/")
print("Original source dataset was not modified.")

spark.stop()
