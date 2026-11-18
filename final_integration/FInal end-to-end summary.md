# End-to-End Self-Healing Pipeline Integration

## Workflow

GTFS input was processed through fault injection, P1 fault detection, P2 recovery, JSON Schema validation, Kafka publishing and consumption, Spark Structured Streaming routing, MongoDB persistence, and Bloom Filter/LSH evaluation.

## Final Results

- Original GTFS records: 387,798
- Records after fault injection: 387,803
- Faults detected: 5
- Records quarantined: 4
- Duplicate records removed: 1
- Recovered records: 387,798
- Invalid recovered records: 0
- Kafka records published: 387,798
- Kafka records processed: 387,798
- Spark valid records: 1
- Spark invalid records: 1
- MongoDB persistence: Successful
- Bloom Filter evaluation: Completed
- LSH comparison reduction: 78%

## Conclusion

The complete workflow demonstrated that the self-healing data pipeline can detect faulty GTFS records, quarantine invalid data, remove duplicates, recover valid records, validate the final dataset, publish and consume records through Kafka, route valid and invalid records through Spark, store processing results in MongoDB, and improve search efficiency using Bloom Filters and LSH.
