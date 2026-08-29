from pathlib import Path

from processing.spark_session import create_spark_session
from quality.checks import (
    assert_not_empty,
    assert_not_null,
    assert_unique,
    assert_value_range,
)

PROJECT_ROOT = Path(__file__).resolve().parents[2]


LATEST_VITALS_GOLD_PATH = (
    PROJECT_ROOT
    / "data"
    / "delta"
    / "gold"
    / "patient_latest_vitals"
)

PATIENT_CONDITION_GOLD_PATH = (
    PROJECT_ROOT
    / "data"
    / "delta"
    / "gold"
    / "patient_conditions"
)

PATIENT_MEDICATIONS_GOLD_PATH = (
    PROJECT_ROOT
    / "data"
    / "delta"
    / "gold"
    / "patient_medications"
)

PATIENT_UTILIZATION_GOLD_PATH = (
    PROJECT_ROOT
    / "data"
    / "delta"
    / "gold"
    / "patient_utilization"
)

def validate_patient_utilization():
    spark = create_spark_session()

    utilization_df = (
        spark.read
        .format("delta")
        .load((str(PATIENT_UTILIZATION_GOLD_PATH)))
    )

    print("Starting Patient Utilization Gold validation...")

    assert_not_empty(
        utilization_df,
        "patient_utilization",
    )

    assert_not_null(
        utilization_df,
        "patient_id",
        "patient_utilization",
    )

    assert_unique(
        utilization_df,
        "patient_id",
        "patient_utilization",
    )

    print("Patient Medications Gold validation passed.")

def validate_patient_medications():
    spark = create_spark_session()

    patient_medications_df = (
        spark.read
        .format("delta")
        .load(str(PATIENT_MEDICATIONS_GOLD_PATH))
    )

    print("Validating Patient Medications Gold...")

    assert_not_empty(
        patient_medications_df,
        "patient_medications",
    )

    assert_not_null(
        patient_medications_df,
        "patient_id",
        "patient_medications",
    )

    assert_unique(
        patient_medications_df,
        "patient_id",
        "patient_medications",
    )

    assert_value_range(
        patient_medications_df,
        "medication_request_count",
        0,
        None,
        "patient_medications",
    )

    assert_value_range(
        patient_medications_df,
        "unique_medication_count",
        0,
        None,
        "patient_medications",
    )

    assert_value_range(
        patient_medications_df,
        "active_medication_count",
        0,
        None,
        "patient_medications",
    )

    print("Patient Medications Gold validation passed.")

def validate_patient_conditions():
    spark = create_spark_session()

    patient_condition_df = (
        spark.read
        .format("delta")
        .load(str(PATIENT_CONDITION_GOLD_PATH))
    )

    print("Validating Patient Conditions Gold...")

    assert_not_empty(
        patient_condition_df,
        "patient_conditions",
    )

    assert_not_null(
        patient_condition_df,
        "patient_id",
        "patient_conditions",
    )

    assert_unique(
        patient_condition_df,
        "patient_id",
        "patient_conditions",
    )

    print("Patient Conditions Gold validation passed.")


def validate_patient_latest_vitals():
    spark = create_spark_session()

    vitals_df = (
        spark.read
        .format("delta")
        .load(str(LATEST_VITALS_GOLD_PATH))
    )

    print("Validating Patient Latest Vitals Gold...")

    assert_not_empty(
        vitals_df,
        "patient_latest_vitals",
    )

    assert_not_null(
        vitals_df,
        "patient_id",
        "patient_latest_vitals",
    )

    assert_unique(
        vitals_df,
        "patient_id",
        "patient_latest_vitals",
    )

    assert_value_range(
        vitals_df,
        "height_cm",
        30,
        275,
        "patient_latest_vitals",
    )

    assert_value_range(
        vitals_df,
        "weight_kg",
        1,
        500,
        "patient_latest_vitals",
    )

    assert_value_range(
        vitals_df,
        "bmi",
        5,
        100,
        "patient_latest_vitals",
    )

    assert_value_range(
        vitals_df,
        "heart_rate",
        20,
        250,
        "patient_latest_vitals",
    )

    assert_value_range(
        vitals_df,
        "systolic_bp",
        40,
        300,
        "patient_latest_vitals",
    )

    assert_value_range(
        vitals_df,
        "diastolic_bp",
        20,
        200,
        "patient_latest_vitals",
    )

    print(
        "Patient Latest Vitals Gold "
        "validation passed."
    )


def main():
    validate_patient_latest_vitals()
    validate_patient_conditions()
    validate_patient_medications()
    validate_patient_utilization()


if __name__ == "__main__":
    main()