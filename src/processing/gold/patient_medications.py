from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from processing.spark_session import create_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[3]

PATIENT_SILVER_PATH = (
    PROJECT_ROOT / "data" / "delta" / "silver" / "patient"
)

MEDICATION_SILVER_PATH = (
    PROJECT_ROOT / "data" / "delta" / "silver" / "medicationrequest"
)

PATIENT_MEDICATIONS_GOLD_PATH = (
    PROJECT_ROOT / "data" / "delta" / "gold" / "patient_medications"
)


def read_silver_patients(
    spark: SparkSession,
) -> DataFrame:
    return (
        spark.read
        .format("delta")
        .load(str(PATIENT_SILVER_PATH))
    )


def read_silver_medications(
    spark: SparkSession,
) -> DataFrame:
    return (
        spark.read
        .format("delta")
        .load(str(MEDICATION_SILVER_PATH))
    )

def summarize_medications(
    medications: DataFrame,
) -> DataFrame:
    return (
        medications
        .groupBy("patient_id")
        .agg(
            F.count("*")
                .alias("medication_request_count"),

            F.countDistinct("medication_code")
                .alias("unique_medication_count"),

            F.sum(
                F.when(
                    F.col("status") == "active",
                    1,
                ).otherwise(0)
            ).alias("active_medication_count"),

            F.collect_set(
                F.when(
                    F.col("status") == "active",
                    F.col("medication_display"),
                )
            ).alias("active_medication_names"),

            F.max("authored_at")
                .alias("latest_medication_authored_at"),
        )
    )


def build_patient_medications(
    patients: DataFrame,
    medication_summary: DataFrame,
) -> DataFrame:
    return (
        patients
        .join(
            medication_summary,
            on="patient_id",
            how="left",
        )
        .fillna(
            {
                "medication_request_count": 0,
                "unique_medication_count": 0,
                "active_medication_count": 0,
            }
        )
        .withColumn(
            "has_active_medications",
            F.col("active_medication_count") > 0,
        )
        .withColumn(
            "gold_processed_at",
            F.current_timestamp(),
        )
    )

def validate_patient_medications(
    patient_medications: DataFrame,
) -> None:
    duplicate_count = (
        patient_medications
        .groupBy("patient_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    null_patient_count = (
        patient_medications
        .filter(F.col("patient_id").isNull())
        .count()
    )

    if duplicate_count:
        raise ValueError(
            f"Found {duplicate_count} duplicate patient IDs."
        )

    if null_patient_count:
        raise ValueError(
            f"Found {null_patient_count} null patient IDs."
        )


def write_patient_medications(
    patient_medications: DataFrame,
) -> None:
    (
        patient_medications.write
        .format("delta")
        .mode("overwrite")
        .save(str(PATIENT_MEDICATIONS_GOLD_PATH))
    )


def main() -> None:
    spark = create_spark_session(
        "patient-medications-gold"
    )

    try:
        patients = read_silver_patients(spark)
        medications = read_silver_medications(spark)

        medication_summary = summarize_medications(
            medications
        )

        patient_medications = build_patient_medications(
            patients,
            medication_summary,
        )

        validate_patient_medications(
            patient_medications
        )

        patient_medications.select(
            "patient_id",
            "birth_date",
            "gender",
            "medication_request_count",
            "unique_medication_count",
            "active_medication_count",
            "has_active_medications",
            "active_medication_names",
            "latest_medication_authored_at",
        ).show(truncate=False)

        print(
            f"Gold patient medication rows: "
            f"{patient_medications.count()}"
        )

        write_patient_medications(
            patient_medications
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()