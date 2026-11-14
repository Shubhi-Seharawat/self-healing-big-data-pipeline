from validation_framework import validate_record

# A valid GTFS stop_times record
valid_record = {
    "trip_id": "TRIP_1001",
    "arrival_time": "08:30:00",
    "departure_time": "08:31:00",
    "stop_id": "STOP_101",
    "stop_sequence": "1",
    "stop_headsign": "",
    "pickup_type": "0",
    "drop_off_type": "0",
    "shape_dist_traveled": "",
    "timepoint": "1"
}

test_cases = {
    "Valid record": valid_record,

    "Missing trip_id": {
        **valid_record,
        "trip_id": ""
    },

    "Invalid arrival_time": {
        **valid_record,
        "arrival_time": "invalid_time"
    },

    "Unexpected pickup_type": {
        **valid_record,
        "pickup_type": "9"
    },

    "Invalid stop_sequence": {
        **valid_record,
        "stop_sequence": "invalid"
    }
}

print("\n========== VALIDATION FRAMEWORK TESTS ==========")

for test_name, record in test_cases.items():
    result = validate_record(record)

    print(f"\nTest: {test_name}")
    print(f"Valid: {result['is_valid']}")

    for error in result["errors"]:
        print(f"Error: {error['field']} - {error['message']}")

print("\n========== TESTING COMPLETED ==========")
