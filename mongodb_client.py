from pymongo import MongoClient
from pymongo.errors import ConnectionFailure

MONGODB_URI = "mongodb://127.0.0.1:27017/"
DATABASE_NAME = "self_healing_pipeline"

COLLECTIONS = [
    "validation_results",
    "detected_faults",
    "quarantine_records",
    "recovery_logs"
]

client = MongoClient(
    MONGODB_URI,
    serverSelectionTimeoutMS=5000
)

def get_database():
    return client[DATABASE_NAME]

def get_collections():
    db = get_database()
    return {
        name: db[name]
        for name in COLLECTIONS
    }

def test_connection():
    try:
        client.admin.command("ping")
        print("MongoDB connection successful!")
        print(f"Database: {DATABASE_NAME}")
        return True
    except ConnectionFailure as error:
        print(f"MongoDB connection failed: {error}")
        return False

if __name__ == "__main__":
    if test_connection():
        print("\nConfigured collections:")
        for name in COLLECTIONS:
            print(f"- {name}")

    client.close()
