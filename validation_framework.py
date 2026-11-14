import json
from pathlib import Path

from jsonschema import Draft202012Validator

SCHEMA_PATH = Path("schemas/gtfs_stop_times.schema.json")


def load_schema():
    with SCHEMA_PATH.open("r", encoding="utf-8") as file:
        schema = json.load(file)

    Draft202012Validator.check_schema(schema)
    return schema


def validate_record(record):
    schema = load_schema()
    validator = Draft202012Validator(schema)

    errors = sorted(
        validator.iter_errors(record),
        key=lambda error: list(error.path)
    )

    return {
        "is_valid": len(errors) == 0,
        "errors": [
            {
                "field": ".".join(str(part) for part in error.path),
                "message": error.message
            }
            for error in errors
        ]
    }


if __name__ == "__main__":
    print("GTFS validation framework loaded successfully.")
    print(f"Schema: {SCHEMA_PATH}")
