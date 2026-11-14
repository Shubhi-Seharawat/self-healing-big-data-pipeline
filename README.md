# Self-Healing Big Data Pipeline with Automatic Data Quality Recovery

## 1. Project Overview

Data quality is an important part of any big data system. When datasets contain missing values, incorrect formats, duplicate records, or unexpected information, these problems can affect the accuracy of analysis and the reliability of downstream applications.

For our DATA 226 group project, we are developing a **Self-Healing Big Data Pipeline with Automatic Data Quality Recovery**.

The main idea behind our project is to build a pipeline that can identify data-quality problems, validate records, separate faulty data, and recover usable records with minimal manual intervention.

We use VTA transit GTFS data as our primary dataset and combine different big data technologies to explore how data quality can be maintained throughout a processing workflow.

Our project brings together Apache Spark, Kafka, Spark Structured Streaming, MongoDB, Schema Registry, and additional data-processing techniques to develop and evaluate a self-healing pipeline.

## 2. Project Objectives

The main objectives of our project are:

- Build a baseline pipeline for processing large datasets.
- Identify different types of data-quality problems.
- Implement and compare P0, P1, and P2 processing approaches.
- Detect missing, invalid, and duplicate records.
- Automatically quarantine faulty records and recover usable data.
- Apply schema-based validation to maintain consistent data structures.
- Explore Bloom filter and Locality-Sensitive Hashing (LSH) techniques as part of the project's similarity and comparison work.
- Use Kafka for data ingestion and streaming communication.
- Integrate Spark Structured Streaming for processing incoming records.
- Use MongoDB to store validation, fault-detection, quarantine, and recovery information.
- Evaluate the different approaches through experiments and measurable results.

## 3. Dataset

We selected the VTA General Transit Feed Specification (GTFS) dataset for our experiments.

The dataset contains four main files:

- `routes.txt`
- `stops.txt`
- `trips.txt`
- `stop_times.txt`

Together, these files contain 399,834 records.

Our primary processing and data-quality experiments focus on `stop_times.txt`, which contains 387,798 original records.

We selected this dataset because it contains structured transit information that allows us to introduce and test different types of data-quality problems.

## 4. Overall Pipeline Architecture

Our project follows a series of processing stages:

1. **Data ingestion:** Load the GTFS dataset into the processing environment.
2. **Baseline processing:** Establish the original dataset and expected record counts.
3. **Fault detection:** Identify data-quality problems using validation rules.
4. **Automatic recovery:** Quarantine invalid records, handle duplicates, and generate a recovered dataset.
5. **Schema validation:** Check whether records follow the expected structure and formats.
6. **Schema management:** Register and manage schema versions.
7. **Streaming:** Use Kafka to move records through the pipeline.
8. **Stream processing:** Use Spark Structured Streaming to validate and route incoming records.
9. **Persistent storage:** Store validation and recovery information in MongoDB.
10. **Evaluation:** Compare the approaches and examine their results, strengths, and limitations.

The goal is to bring these stages together into a workflow that can handle data-quality issues more systematically.

## 5. Major Project Components

### 5.1 P0 – Baseline Pipeline

We started by building a basic processing pipeline using Apache Spark.

The original GTFS `stop_times` dataset was loaded into Spark and stored in Parquet format. We then compared the source and output record counts to establish a baseline for our subsequent experiments.

The pipeline successfully preserved all 387,798 original records.

### 5.2 P1 – Data Quality Fault Detection

After establishing the baseline, we introduced controlled data-quality problems to test the detection capabilities of our pipeline.

Our detection framework covers five types of faults:

1. Missing `trip_id`.
2. Invalid `arrival_time`.
3. Duplicate records.
4. Unexpected `pickup_type` values.
5. Invalid `stop_sequence`.

Apache Spark was used to process the dataset and generate separate reports for the detected issues.

In our final controlled fault-detection test, the pipeline processed 387,803 records and detected one occurrence of each of the five injected fault types.

### 5.3 P2 – Automatic Data Recovery

The next stage focuses on handling the problems identified during fault detection.

Instead of allowing invalid records to continue through the pipeline, we separate them into a quarantine dataset. We also remove duplicate records to produce a clean recovered dataset.

In our controlled recovery experiment:

| Measurement | Result |
|---|---:|
| Input records | 387,803 |
| Quarantined records | 4 |
| Valid records before deduplication | 387,799 |
| Duplicate records removed | 1 |
| Recovered records | 387,798 |

We also verified the saved Parquet outputs to ensure that the recovery results matched the pipeline's recovery summary.

### 5.4 Data Validation and Schema Management

We implemented JSON Schema-based validation to check the structure and expected formats of GTFS records.

The framework was first tested on a sample of 100 records and subsequently extended to the complete dataset of 387,798 records. All original records passed the full-dataset validation.

We also integrated Confluent Schema Registry to manage the GTFS JSON Schema. We registered the schema, retrieved it through the Registry API, and tested schema versioning.

Kafka serialization and deserialization were also tested using the registered schema.

### 5.5 Bloom Filters and Locality-Sensitive Hashing

Bloom filters and Locality-Sensitive Hashing (LSH) form part of the project's additional similarity-detection and comparison work.

These techniques are relevant when working with large datasets because they provide ways to identify potentially similar records without always comparing every record directly.

A Bloom filter is a probabilistic data structure used to test whether an element may belong to a set. It can reduce unnecessary membership checks, although false positives are possible.

Locality-Sensitive Hashing is used to identify potentially similar items by increasing the likelihood that similar items are mapped to the same buckets.

Our group includes these techniques in the broader project evaluation to explore their usefulness for large-scale data processing and similarity detection.

### 5.6 Kafka-Based Data Ingestion and Validation

We integrated Apache Kafka to support the movement of records through the pipeline.

The streaming workflow uses separate topics for different stages:

- `gtfs-raw` – Receives incoming GTFS records.
- `gtfs-validated` – Stores records that pass validation.
- `gtfs-quarantine` – Stores records that fail validation.
- `gtfs-recovery-events` – Supports recovery-related events.

Our Kafka producer successfully published 387,798 original records to the raw topic. The consumer then processed all 387,798 records using the JSON Schema validation framework.

All records passed validation during the original-dataset test. We also performed a separate controlled fault test and verified that an invalid record was routed to the quarantine topic with its validation error.

### 5.7 Spark Structured Streaming

We integrated Spark Structured Streaming with Kafka to process records from streaming topics.

The validation workflow was tested using two records: one valid record and one containing an intentionally incorrect arrival time.

Spark processed both records using the existing validation framework. The valid record was routed to the validated topic, while the invalid record was sent to the quarantine topic with its corresponding validation error.

We independently checked both topics to verify that the routing worked correctly.

Our current demonstration uses the `availableNow` trigger, which processes available records in bounded micro-batches before completing the run.

### 5.8 MongoDB Integration

We integrated MongoDB using PyMongo to store information generated by different stages of the pipeline.

The database contains four collections:

- `validation_results`
- `detected_faults`
- `quarantine_records`
- `recovery_logs`

We tested the insertion and retrieval of validation results, P1 fault-detection records, P2 quarantine records, and recovery summaries.

This allows the pipeline's results to be stored and inspected independently of the processing jobs.

## 6. Experimental Results

The following table summarizes the main results from our completed data-processing and streaming tests.

| Measurement | Result |
|---|---:|
| Original `stop_times` records | 387,798 |
| Records after fault injection | 387,803 |
| Fault types detected | 5 |
| Invalid records quarantined | 4 |
| Duplicate records removed | 1 |
| Recovered records | 387,798 |
| Full-dataset JSON Schema validation | 387,798 passed |
| Kafka records published | 387,798 |
| Kafka records validated | 387,798 |
| Controlled Spark streaming test | 1 valid and 1 invalid |

The Bloom filter and LSH comparison results are maintained as a separate evaluation workstream and should be reported using the group's actual experimental measurements.

## 7. Technologies Used

Our project uses the following technologies:

- **Python:** Implementation of the pipeline and supporting scripts.
- **Apache Spark / PySpark:** Distributed data processing and validation.
- **Apache Kafka:** Data ingestion and streaming communication.
- **Spark Structured Streaming:** Processing and validation of streaming records.
- **Confluent Schema Registry:** Schema registration and version management.
- **JSON Schema:** Record structure and data validation.
- **MongoDB / PyMongo:** Storage of validation and recovery information.
- **Parquet:** Storage of processed datasets.
- **Docker and Docker Compose:** Running the supporting infrastructure.
- **AWS EC2:** Cloud environment for project implementation and testing.
- **Git and GitHub:** Source-code management and collaboration.

## 8. Repository Structure

The repository contains implementation scripts, schema definitions, infrastructure configuration, and project evidence.

Some of the main files include:

| File | Purpose |
|---|---|
| `p1_detection.py` | Detects data-quality faults |
| `p2_recovery.py` | Performs quarantine and recovery |
| `validation_framework.py` | Implements JSON Schema validation |
| `spark_validation.py` | Performs Spark-based validation |
| `spark_validation_full.py` | Validates the complete dataset |
| `mongodb_client.py` | Configures the MongoDB connection |
| `mongodb_p1_integration.py` | Stores P1 detection results |
| `mongodb_p2_integration.py` | Stores P2 quarantine and recovery results |
| `kafka_producer.py` | Publishes records to Kafka |
| `kafka_consumer.py` | Consumes and validates Kafka records |
| `schema_registry_kafka_test.py` | Tests Kafka and Schema Registry integration |
| `kafka_fault_test.py` | Tests controlled fault detection and quarantine |
| `spark_kafka_streaming.py` | Implements Spark Structured Streaming validation and routing |

## 9. Project Evidence

We have maintained a separate implementation evidence document containing screenshots and explanations of the major stages of our work.

It covers:

- Dataset loading and P0 baseline processing.
- P1 fault detection and controlled fault injection.
- P2 quarantine and automatic recovery.
- JSON Schema validation and Schema Registry.
- MongoDB integration and record verification.
- Kafka topic configuration, ingestion, and validation.
- Controlled Kafka fault detection.
- Spark Structured Streaming and record routing.

The evidence document provides a visual record of the implementation and the results obtained during testing.

## 10. Project Status and Future Work

Our group has developed several major building blocks of the self-healing pipeline, including baseline processing, fault detection, recovery, schema validation, streaming integration, and persistent storage.

The broader project also includes Bloom filter and LSH evaluation, comparison of approaches, and overall system integration.

The final project evaluation will bring together the results from all workstreams and examine the effectiveness, scalability, and limitations of the different techniques.

## 11. Conclusion

Through this project, we are exploring how different big data technologies can work together to improve data quality and pipeline reliability.

The work begins with identifying problems in a dataset and extends to validating, isolating, and recovering records. By combining Spark, Kafka, Schema Registry, MongoDB, and additional similarity-detection techniques, our group aims to develop a pipeline that can handle data-quality problems more systematically and reduce the need for manual intervention.