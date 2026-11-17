from datetime import datetime, timezone
from mongodb_client import get_database, test_connection, client

def main():
    if not test_connection():
        return

    db = get_database()
    collection = db["validation_results"]

    test_record = {
        "test_id": "DATA226-MONGO-001",
        "component": "JSON Schema Validation",
        "dataset": "GTFS stop_times",
        "total_records": 387798,
        "valid_records": 387798,
        "invalid_records": 0,
        "status": "SUCCESS",
        "created_at": datetime.now(timezone.utc)
    }

    result = collection.insert_one(test_record)
    print(f"\nInserted document ID: {result.inserted_id}")

    retrieved = collection.find_one({
        "test_id": "DATA226-MONGO-001"
    })

    print("\nRETRIEVED MONGODB RECORD")
    print("-----------------------")
    print(f"Component: {retrieved['component']}")
    print(f"Total records: {retrieved['total_records']}")
    print(f"Valid records: {retrieved['valid_records']}")
    print(f"Invalid records: {retrieved['invalid_records']}")
    print(f"Status: {retrieved['status']}")

    client.close()

if __name__ == "__main__":
    main()
