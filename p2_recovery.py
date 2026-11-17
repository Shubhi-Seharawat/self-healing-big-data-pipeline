from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when

spark = (
    SparkSession.builder
    .appName("P2-Automatic-Recovery")
    .master("local[*]")
    .getOrCreate()
)

# Read the controlled faulty dataset
input_path = "outputs/p1/faulty_stop_times"

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv(input_path)
)

# Define the five data-quality conditions
missing_id = (
    col("trip_id").isNull() | (col("trip_id") == "")
)

invalid_time = (
    col("arrival_time").isNull()
    | ~col("arrival_time").rlike(r"^\d{2}:\d{2}:\d{2}$")
)

unexpected_pickup = (
    col("pickup_type").isNull()
    | ~col("pickup_type").isin("0", "1", "2", "3")
)

invalid_sequence = (
    col("stop_sequence").isNull()
    | ~col("stop_sequence").rlike(r"^[1-9]\d*$")
)

# Combine invalid-record conditions
invalid_condition = (
    missing_id
    | invalid_time
    | unexpected_pickup
    | invalid_sequence
)

# Separate invalid records for quarantine
quarantine_df = (
    df.filter(invalid_condition)
    .withColumn(
        "quarantine_reason",
        when(missing_id, lit("MISSING_TRIP_ID"))
        .when(invalid_time, lit("INVALID_ARRIVAL_TIME"))
        .when(unexpected_pickup, lit("UNEXPECTED_PICKUP_TYPE"))
        .when(invalid_sequence, lit("INVALID_STOP_SEQUENCE"))
    )
)

# Keep only valid records
valid_df = df.filter(~invalid_condition)

# Remove exact duplicate records, retaining one copy
recovered_df = valid_df.dropDuplicates(df.columns)

# Calculate recovery statistics
input_count = df.count()
quarantine_count = quarantine_df.count()
valid_count = valid_df.count()
recovered_count = recovered_df.count()
duplicates_removed = valid_count - recovered_count

# Save recovered data and quarantine separately
recovered_df.write.mode("overwrite").parquet(
    "outputs/p2/recovered"
)

quarantine_df.write.mode("overwrite").parquet(
    "outputs/p2/quarantine"
)

print("\n========== P2 AUTOMATIC RECOVERY RESULTS ==========")
print(f"Input records: {input_count}")
print(f"Quarantined records: {quarantine_count}")
print(f"Valid records before deduplication: {valid_count}")
print(f"Duplicate records removed: {duplicates_removed}")
print(f"Recovered records: {recovered_count}")
print(f"Recovery output: outputs/p2/recovered")
print(f"Quarantine output: outputs/p2/quarantine")

# Verify the saved outputs
saved_recovered = spark.read.parquet("outputs/p2/recovered").count()
saved_quarantine = spark.read.parquet("outputs/p2/quarantine").count()

print("\n========== OUTPUT VERIFICATION ==========")
print(f"Saved recovered records: {saved_recovered}")
print(f"Saved quarantine records: {saved_quarantine}")
print(f"Recovered count matches: {saved_recovered == recovered_count}")
print(f"Quarantine count matches: {saved_quarantine == quarantine_count}")

spark.stop()
