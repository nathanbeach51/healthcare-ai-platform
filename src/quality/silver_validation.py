from pathlib import Path

from pyspark.sql import SparkSession

from processing.spark_session import create_spark_session
from quality.checks import (
    assert_not_empty,
    assert_not_null,
    assert_unique,
    assert_column_lte,
    assert_unique_combination,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]

PATIENT_SILVER_PATH = (
    PROJECT_ROOT / "data" / "delta" / "silver" / "patient"
)

ENCOUNTER_SILVER_PATH = (
    PROJECT_ROOT / "data" / "delta" / "silver" / "encounter"
)

CONDITION_SILVER_PATH = (
    PROJECT_ROOT / "data" / "delta" / "silver" / "condition"
)

OBSERVATION_SILVER_PATH = (
    PROJECT_ROOT / "data" / "delta" / "silver" / "observation"
)

MEDICATION_SILVER_PATH = (
    PROJECT_ROOT / "data" / "delta" / "silver" / "medicationrequest"
)


def validate_medication_silver(
    spark: SparkSession,
) -> None:
    medication_df = (
        spark.read
        .format("delta")
        .load(str(MEDICATION_SILVER_PATH))
    )

    print("Validating MedicationRequest Silver...")

    assert_not_empty(
        medication_df,
        "medication_request_silver",
    )

    assert_not_null(
        medication_df,
        "medication_request_id",
        "medication_request_silver",
    )

    assert_unique(
        medication_df,
        "medication_request_id",
        "medication_request_silver",
    )

    assert_not_null(
        medication_df,
        "patient_id",
        "medication_request_silver",
    )

    print("MedicationRequest Silver validation passed.")


def validate_observation_silver(
    spark: SparkSession,
) -> None:
    observation_df = (
        spark.read
        .format("delta")
        .load(str(OBSERVATION_SILVER_PATH))
    )

    print("Validating Observation Silver...")

    assert_not_empty(
        observation_df,
        "observation_silver",
    )

    assert_not_null(
        observation_df,
        "observation_id",
        "observation_silver",
    )

    assert_not_null(
        observation_df,
        "patient_id",
        "observation_silver",
    )

    assert_unique_combination(
        observation_df,
        ["observation_id", "observation_code"],
        "observation_silver",
    )

    print("Observation Silver validation passed.")


def validate_condition_silver(
    spark: SparkSession,
) -> None:
    condition_df = (
        spark.read
        .format("delta")
        .load(str(CONDITION_SILVER_PATH))
    )

    print("Validating Condition Silver...")

    assert_not_empty(
        condition_df,
        "condition_silver",
    )

    assert_not_null(
        condition_df,
        "condition_id",
        "condition_silver",
    )

    assert_unique(
        condition_df,
        "condition_id",
        "condition_silver",
    )

    assert_not_null(
        condition_df,
        "patient_id",
        "condition_silver",
    )

    print("Condition Silver validation passed.")


def validate_encounter_silver(
    spark: SparkSession,
) -> None:
    encounter_df = (
        spark.read
        .format("delta")
        .load(str(ENCOUNTER_SILVER_PATH))
    )

    print("Validating Encounter Silver...")

    assert_not_empty(
        encounter_df,
        "encounter_silver",
    )

    assert_not_null(
        encounter_df,
        "encounter_id",
        "encounter_silver",
    )

    assert_unique(
        encounter_df,
        "encounter_id",
        "encounter_silver",
    )

    assert_not_null(
        encounter_df,
        "patient_id",
        "encounter_silver",
    )

    assert_column_lte(
        encounter_df,
        "encounter_start",
        "encounter_end",
        "encounter_silver",
    )

    print("Encounter Silver validation passed.")


def validate_patient_silver(
    spark: SparkSession,
) -> None:
    patient_df = (
        spark.read
        .format("delta")
        .load(str(PATIENT_SILVER_PATH))
    )

    print("Validating Patient Silver...")

    assert_not_empty(
        patient_df,
        "patient_silver",
    )

    assert_not_null(
        patient_df,
        "patient_id",
        "patient_silver",
    )

    assert_unique(
        patient_df,
        "patient_id",
        "patient_silver",
    )

    print("Patient Silver validation passed.")


def main() -> None:
    spark = create_spark_session(
        "silver-validation"
    )

    try:
        validate_patient_silver(spark)
        validate_encounter_silver(spark)
        validate_condition_silver(spark)
        validate_observation_silver(spark)
        validate_medication_silver(spark)

        print("Silver validation completed successfully.")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()