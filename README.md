# Self-Healing Big Data Pipeline with Automatic Data Quality Recovery

## Project Overview

This project aims to develop a PySpark-based big data pipeline that detects data quality problems and applies suitable recovery actions. The initial prototype uses VTA transit GTFS data and runs in Google Colab.

## Dataset

Four GTFS files are used in the initial experiment:

- `routes.txt`
- `stops.txt`
- `trips.txt`
- `stop_times.txt`

Together, the four tables contain 399,834 rows in the downloaded dataset.

## Current Implementation

- Set up PySpark in Google Colab.
- Loaded the four GTFS tables and inspected their schemas and row counts.
- Checked missing and blank values in the `stop_times` table.
- Created a P0 baseline and verified that the Parquet output preserved all 387,798 rows.
- Injected three controlled data quality faults: a missing trip identifier, an invalid arrival time, and a duplicate record.
- Implemented detection rules and corrected the duplicate detection logic using a Spark window function.
- Quarantined two invalid records and removed the extra duplicate.
- Saved and verified the recovered dataset and quarantine records in Parquet format.

## Preliminary Results

| Measurement | Result |
|---|---:|
| Original `stop_times` rows | 387,798 |
| P0 baseline rows | 387,798 |
| Fault-injected rows | 387,799 |
| Quarantined records | 2 |
| Recovered rows | 387,796 |
| Controlled fault types detected | 3 of 3 |
| Preliminary detection precision | 100% |
| Preliminary detection recall | 100% |

The detection metrics apply only to the three deliberately injected fault events. Broader testing is needed before drawing conclusions about overall pipeline performance.

## Technologies Used

- Python
- PySpark / Apache Spark
- Google Colab
- Parquet
- GitHub

## Evidence

The `evidence/` folder contains the project development evidence PDF and its README.

## Planned Work

- Implement and compare P0, P1, and P2 pipeline configurations.
- Test additional data quality problems.
- Evaluate repair accuracy, quarantine rate, processing time, and resource usage.
- Implement and test the planned Kafka streaming and quarantine-storage components.
- Continue the warehouse evaluation and test the pipeline at larger data sizes.
- Complete the additional techniques, experiments, documentation, and demonstrations required by the project scope and course rubric.

## Project Status

The initial controlled prototype is working. Further integration, testing, and evaluation remain in progress.
