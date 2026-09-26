"""
Benchmark Spark persistence against the Observation Silver workload.

Compares repeated actions against the Observation Silver DataFrame
with and without persistence. This benchmark intentionally reuses
production transformation and validation logic while avoiding writes
to production Silver Delta state.
"""

import statistics
import time
from pathlib import Path

from pyspark import StorageLevel

from processing.spark_session import create_spark_session

from processing.silver.observation_transform import (
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

RUNS = 3


def build_silver(bronze):
    simple = transform_simple_observations(bronze)

    components = transform_component_observations(bronze)

    combined = combine_observations(
        simple,
        components,
    )

    current = deduplicate_observations(
        combined
    )

    return add_silver_metadata(current)


def run_workload(silver):
    """
    Execute the repeated actions performed against the Silver
    Observation DataFrame.

    Validation performs several Spark actions, followed by a
    separate row count.
    """

    validate_observations(silver)

    silver_count = silver.count()

    return silver_count


def benchmark_baseline(bronze):
    silver = build_silver(bronze)

    start = time.perf_counter()

    silver_count = run_workload(silver)

    elapsed = time.perf_counter() - start

    return elapsed, silver_count


def benchmark_persisted(bronze):
    silver = build_silver(bronze)

    silver.persist(StorageLevel.MEMORY_AND_DISK)

    start = time.perf_counter()

    silver_count = run_workload(silver)

    elapsed = time.perf_counter() - start

    silver.unpersist()

    return elapsed, silver_count


def main():
    spark = create_spark_session(
        "benchmark-observation-persistence"
    )

    try:
        bronze = (
            spark.read
            .format("delta")
            .load(str(PERFORMANCE_PATH))
        )

        bronze_count = bronze.count()

        print(
            "\nObservation Persistence Benchmark"
        )
        print(
            "================================="
        )
        print(f"Bronze rows: {bronze_count:,}")
        print(
            f"Bronze partitions: "
            f"{bronze.rdd.getNumPartitions()}"
        )

        baseline_times = []
        persisted_times = []

        print("\nBaseline")
        print("--------")

        for run in range(1, RUNS + 1):
            elapsed, silver_count = (
                benchmark_baseline(bronze)
            )

            baseline_times.append(elapsed)

            print(
                f"Run {run}: {elapsed:.2f} seconds"
            )

        print("\nPersisted")
        print("---------")

        for run in range(1, RUNS + 1):
            elapsed, silver_count = (
                benchmark_persisted(bronze)
            )

            persisted_times.append(elapsed)

            print(
                f"Run {run}: {elapsed:.2f} seconds"
            )

        baseline_median = statistics.median(
            baseline_times
        )

        persisted_median = statistics.median(
            persisted_times
        )

        improvement = (
            (
                baseline_median
                - persisted_median
            )
            / baseline_median
            * 100
        )

        print("\nResults")
        print("-------")
        print(
            f"Silver rows: {silver_count:,}"
        )
        print(
            f"Baseline median: "
            f"{baseline_median:.2f} seconds"
        )
        print(
            f"Persisted median: "
            f"{persisted_median:.2f} seconds"
        )
        print(
            f"Runtime change: "
            f"{improvement:.1f}% faster"
        )

        input(
            "\nPress Enter to stop Spark..."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()