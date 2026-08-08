from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from processing.spark_session import create_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[3]

PATIENT_SILVER_PATH = (
    PROJECT_ROOT / "data" / "delta" / "silver" / "patient"
)

CONDITION_SILVER_PATH = (
    PROJECT_ROOT / "data" / "delta" / "silver" / "condition"
)

PATIENT_CONDITIONS_GOLD_PATH = (
    PROJECT_ROOT / "data" / "delta" / "gold" / "patient_conditions"
)


def read_silver_patients(
    spark: SparkSession,
) -> DataFrame:
    return (
        spark.read
        .format("delta")
        .load(str(PATIENT_SILVER_PATH))
    )


def read_silver_conditions(
    spark: SparkSession,
) -> DataFrame:
    return (
        spark.read
        .format("delta")
        .load(str(CONDITION_SILVER_PATH))
    )


def summarize_conditions(
    conditions: DataFrame,
) -> DataFrame:
    return (
        conditions
        .groupBy("patient_id")
        .agg(
            F.count("*").alias("condition_count"),

            F.sum(
                F.when(
                    F.col("is_active"),
                    1,
                ).otherwise(0)
            ).alias("active_condition_count"),

            F.min("onset_at").alias(
                "first_condition_onset_at"
            ),

            F.max("onset_at").alias(
                "latest_condition_onset_at"
            ),

            F.collect_set(
                "condition_display"
            ).alias("condition_names"),
        )
    )


def build_patient_conditions(
    patients: DataFrame,
    condition_summary: DataFrame,
) -> DataFrame:
    return (
        patients
        .join(
            condition_summary,
            on="patient_id",
            how="left",
        )
        .fillna(
            {
                "condition_count": 0,
                "active_condition_count": 0,
            }
        )
        .withColumn(
            "has_active_condition",
            F.col("active_condition_count") > 0,
        )
        .withColumn(
            "gold_processed_at",
            F.current_timestamp(),
        )
    )


def validate_patient_conditions(
    patient_conditions: DataFrame,
) -> None:
    duplicate_count = (
        patient_conditions
        .groupBy("patient_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    null_patient_id_count = (
        patient_conditions
        .filter(F.col("patient_id").isNull())
        .count()
    )

    if duplicate_count:
        raise ValueError(
            f"Found {duplicate_count} duplicate patient IDs "
            "in Gold patient conditions."
        )

    if null_patient_id_count:
        raise ValueError(
            f"Found {null_patient_id_count} null patient IDs "
            "in Gold patient conditions."
        )


def write_patient_conditions(
    patient_conditions: DataFrame,
) -> None:
    (
        patient_conditions.write
        .format("delta")
        .mode("overwrite")
        .save(str(PATIENT_CONDITIONS_GOLD_PATH))
    )


def main() -> None:
    spark = create_spark_session(
        "patient-conditions-gold"
    )

    try:
        patients = read_silver_patients(spark)
        conditions = read_silver_conditions(spark)

        condition_summary = summarize_conditions(
            conditions
        )

        patient_conditions = build_patient_conditions(
            patients,
            condition_summary,
        )

        validate_patient_conditions(
            patient_conditions
        )

        patient_conditions.select(
            "patient_id",
            "birth_date",
            "gender",
            "condition_count",
            "active_condition_count",
            "has_active_condition",
            "first_condition_onset_at",
            "latest_condition_onset_at",
            "condition_names",
        ).show(truncate=False)

        print(
            f"Patient condition rows: "
            f"{patient_conditions.count()}"
        )

        write_patient_conditions(
            patient_conditions
        )

        saved = (
            spark.read
            .format("delta")
            .load(str(PATIENT_CONDITIONS_GOLD_PATH))
        )

        print(
            f"Saved Gold patient condition rows: "
            f"{saved.count()}"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()