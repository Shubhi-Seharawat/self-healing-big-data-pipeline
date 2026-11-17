from pyspark.sql import SparkSession
from jsonschema import Draft202012Validator
from validation_framework import load_schema

def validate_partition(rows):
    validator = Draft202012Validator(schema)

    valid_count = 0
    invalid_count = 0
    examples = []

    for row in rows:
        record = row.asDict(recursive=True)
        errors = list(validator.iter_errors(record))

        if errors:
            invalid_count += 1

            if len(examples) < 5:
                examples.append({
                    "record": record,
                    "errors": [
                        {
                            "field": ".".join(str(part) for part in error.path),
                            "message": error.message
                        }
                        for error in errors
                    ]
                })
        else:
            valid_count += 1

    yield (valid_count, invalid_count, examples)


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("FullDatasetJSONSchemaValidation")
        .master("local[*]")
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("ERROR")

    # Load the JSON Schema once on the driver.
    schema = load_schema()

    # Make the schema available to Spark worker processes.
    schema_broadcast = spark.sparkContext.broadcast(schema)

    # Use the broadcast schema inside each partition.
    def validate_partition(rows):
        validator = Draft202012Validator(schema_broadcast.value)

        valid_count = 0
        invalid_count = 0
        examples = []

        for row in rows:
            record = row.asDict(recursive=True)
            errors = list(validator.iter_errors(record))

            if errors:
                invalid_count += 1

                if len(examples) < 5:
                    examples.append({
                        "errors": [
                            {
                                "field": ".".join(str(part) for part in error.path),
                                "message": error.message
                            }
                            for error in errors
                        ]
                    })
            else:
                valid_count += 1

        yield (valid_count, invalid_count, examples)

    df = (
        spark.read
        .option("header", True)
        .option("inferSchema", False)
        .csv("data/gtfs/stop_times.txt")
    )

    print("\nStarting full-dataset JSON Schema validation...")
    print(f"Input records: {df.count():,}")

    # Each Spark partition validates its own records.
    partition_results = df.rdd.mapPartitions(validate_partition).collect()

    total_valid = sum(result[0] for result in partition_results)
    total_invalid = sum(result[1] for result in partition_results)
    total_records = total_valid + total_invalid

    print("\nFULL DATASET VALIDATION RESULTS")
    print("--------------------------------")
    print(f"Total records: {total_records:,}")
    print(f"Valid records: {total_valid:,}")
    print(f"Invalid records: {total_invalid:,}")
    print(f"Validation success: {total_invalid == 0}")

    invalid_examples = [
        example
        for result in partition_results
        for example in result[2]
    ][:5]

    if invalid_examples:
        print("\nSample validation errors:")
        for index, example in enumerate(invalid_examples, start=1):
            print(f"{index}. {example}")

    schema_broadcast.destroy()
    spark.stop()
