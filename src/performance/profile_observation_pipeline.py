"""
Performance profiler for the Observation Spark pipeline.

Reuses the production Observation transformation logic against the full
Bronze dataset so Spark execution plans, partitioning, shuffles, and
execution behavior can be analyzed without modifying incremental
pipeline state.
"""

import time

from processing.spark_session import (
    create_spark_session,
)

from pathlib import Path

from pyspark.sql import functions as F

from processing.silver.observation_transform import (
    read_bronze_observations,
    transform_simple_observations,
    transform_component_observations,
    combine_observations,
    deduplicate_observations,
    add_silver_metadata,
    validate_observations,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

PERFORMANCE_PATH = (
    PROJECT_ROOT
    / "data"
    / "performance"
    / "observation_5m_by_patient"
)


def main() -> None:
    spark = create_spark_session(
        "profile-observation-file-layout"
    )

    try:
        start_time = time.perf_counter()

        bronze = (
            spark.read
            .format("delta")
            .load(str(PERFORMANCE_PATH))
        )

        patient_id = "200009"

        start_time = time.perf_counter()

        patient_observations = (
            bronze
            .filter(
            F.col("patient_id") == patient_id
    )
)

        patient_count = patient_observations.count()

        elapsed = time.perf_counter() - start_time

        print("\nPatient Observation Query")
        print("-------------------------")
        print(f"Patient ID: {patient_id}")
        print(f"Rows returned: {patient_count:,}")
        print(
            f"Query time: {elapsed:.2f} seconds"
        )

        print("\nPhysical Plan")
        print("-------------")
        patient_observations.explain("formatted")

        input(
            "\nPress Enter to stop Spark..."
        )

        bronze_count = bronze.count()

        elapsed = time.perf_counter() - start_time

        print("\nObservation File Layout Test")
        print("----------------------------")
        print(f"Bronze rows: {bronze_count:,}")
        print(
            f"Bronze partitions: "
            f"{bronze.rdd.getNumPartitions()}"
        )
        print(
            f"Read + count time: "
            f"{elapsed:.2f} seconds"
        )

        input(
            "\nPress Enter to stop Spark..."
        )

    finally:
        spark.stop()

if __name__ == "__main__":
    main()