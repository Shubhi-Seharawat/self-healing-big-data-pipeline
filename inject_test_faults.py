from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

spark = (
    SparkSession.builder
    .appName("P1-Controlled-Fault-Injection")
    .master("local[*]")
    .getOrCreate()
)

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv("data/gtfs/stop_times.txt")
)

sample_rows = df.limit(5).collect()

if len(sample_rows) < 5:
    raise ValueError("At least 5 source records are required.")

# Fault 1: Missing trip_id
missing_id_row = (
    spark.createDataFrame([sample_rows[0]], schema=df.schema)
    .withColumn("trip_id", lit(None).cast("string"))
)

# Fault 2: Invalid arrival_time
invalid_time_row = (
    spark.createDataFrame([sample_rows[1]], schema=df.schema)
    .withColumn("arrival_time", lit("invalid_time"))
)

# Fault 3: Exact duplicate
duplicate_row = spark.createDataFrame(
    [sample_rows[2]], schema=df.schema
)

# Fault 4: Unexpected pickup_type
unexpected_pickup_row = (
    spark.createDataFrame([sample_rows[3]], schema=df.schema)
    .withColumn("pickup_type", lit("9"))
)

# Fault 5: Invalid stop_sequence
invalid_sequence_row = (
    spark.createDataFrame([sample_rows[4]], schema=df.schema)
    .withColumn("stop_sequence", lit("invalid"))
)

test_df = (
    df.unionByName(missing_id_row)
      .unionByName(invalid_time_row)
      .unionByName(duplicate_row)
      .unionByName(unexpected_pickup_row)
      .unionByName(invalid_sequence_row)
)

output_path = "outputs/p1/faulty_stop_times"

test_df.write.mode("overwrite").option("header", True).csv(output_path)

print("\n========== FIVE-FAULT INJECTION RESULTS ==========")
print(f"Original records: {df.count()}")
print(f"Records after injection: {test_df.count()}")
print(f"Injected fault records: {test_df.count() - df.count()}")
print(f"Output: {output_path}")

spark.stop()
