"""
Profile the Gold patient latest vitals transformation.

This script runs the production Gold transformation logic against
the scaled Silver Observation performance dataset without modifying
the production Gold Delta table.
"""

import time
from pathlib import Path

from processing.spark_session import create_spark_session
from processing.gold.patient_latest_vitals import (
    read_silver_patients,
    filter_vitals,
    get_latest_vitals,
    add_vital_name,
    pivot_vitals,
    build_patient_latest_vitals,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]

OBSERVATION_PERFORMANCE_PATH = (
    PROJECT_ROOT
    / "data"
    / "performance"
    / "observation_silver_5m"
)

PERFORMANCE_GOLD_PATH = (
    PROJECT_ROOT
    / "data"
    / "performance"
    / "patient_latest_vitals_gold"
)


def main() -> None:
    spark = create_spark_session(
        "profile-patient-latest-vitals"
    )

    try:
        observations = (
            spark.read
            .format("delta")
            .load(str(OBSERVATION_PERFORMANCE_PATH))
        )

        patients = read_silver_patients(spark)

        print("\nPatient Latest Vitals Baseline")
        print("------------------------------")

        print(
            f"Observation partitions: "
            f"{observations.rdd.getNumPartitions()}"
        )

        print(
            f"Patient partitions: "
            f"{patients.rdd.getNumPartitions()}"
        )

        vitals = filter_vitals(
            observations
        )

        latest = get_latest_vitals(
            vitals
        )

        named = add_vital_name(
            latest
        )

        pivoted = pivot_vitals(
            named
        )

        patient_vitals = build_patient_latest_vitals(
            patients,
            pivoted,
        )

        print("\nPhysical Plan")
        print("-------------")

        patient_vitals.explain(
            "formatted"
        )

        start_time = time.perf_counter()

        (
            patient_vitals.write
            .format("delta")
            .mode("overwrite")
            .save(str(PERFORMANCE_GOLD_PATH))
        )

        elapsed = time.perf_counter() - start_time

        print(
            f"\nGold write time: "
            f"{elapsed:.2f} seconds"
        )

        input(
            "\nPress Enter to stop Spark..."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()