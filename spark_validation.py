from pyspark.sql import SparkSession
from validation_framework import validate_record

spark = (
    SparkSession.builder
    .appName("Spark-JSONSchema-Validation")
    .master("local[*]")
    .getOrCreate()
)

df = (
    spark.read
    .option("header", True)
    .option("inferSchema", False)
    .csv("data/gtfs/stop_times.txt")
)

# Validate a sample of actual GTFS records
sample_records = df.limit(100).collect()

valid_count = 0
invalid_count = 0
invalid_examples = []

for row in sample_records:
    result = validate_record(row.asDict())

    if result["is_valid"]:
        valid_count += 1
    else:
        invalid_count += 1
        if len(invalid_examples) < 5:
            invalid_examples.append(result["errors"])

print("\n========== SPARK VALIDATION RESULTS ==========")
print(f"Records sampled: {len(sample_records)}")
print(f"Valid records: {valid_count}")
print(f"Invalid records: {invalid_count}")

for index, errors in enumerate(invalid_examples, start=1):
    print(f"\nInvalid example {index}:")
    for error in errors:
        print(error)

spark.stop()
