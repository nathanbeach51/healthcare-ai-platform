from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from processing.spark_session import create_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[3]

PATIENT_SILVER_PATH = (
    PROJECT_ROOT / "data" / "delta" / "silver" / "patient"
)

ENCOUNTER_SILVER_PATH = (
    PROJECT_ROOT / "data" / "delta" / "silver" / "encounter"
)

PATIENT_UTILIZATION_GOLD_PATH = (
    PROJECT_ROOT / "data" / "delta" / "gold" / "patient_utilization"
)


def read_silver_patients(spark: SparkSession) -> DataFrame:
    return (
        spark.read
        .format("delta")
        .load(str(PATIENT_SILVER_PATH))
    )


def read_silver_encounters(spark: SparkSession) -> DataFrame:
    return (
        spark.read
        .format("delta")
        .load(str(ENCOUNTER_SILVER_PATH))
    )


def summarize_encounters(encounters: DataFrame) -> DataFrame:
    return (
        encounters
        .groupBy("patient_id")
        .agg(
            F.count("*").alias("encounter_count"),
            F.min("encounter_start").alias("first_encounter_at"),
            F.max("encounter_start").alias("latest_encounter_at"),
            F.avg("encounter_duration_minutes")
                .alias("average_encounter_duration_minutes"),
        )
    )


def build_patient_utilization(
    patients: DataFrame,
    encounter_summary: DataFrame,
) -> DataFrame:
    return (
        patients
        .join(
            encounter_summary,
            on="patient_id",
            how="left",
        )
        .fillna(
            {
                "encounter_count": 0,
                "average_encounter_duration_minutes": 0.0,
            }
        )
        .withColumn(
            "gold_processed_at",
            F.current_timestamp(),
        )
    )


def write_patient_utilization(dataframe: DataFrame) -> None:
    (
        dataframe.write
        .format("delta")
        .mode("overwrite")
        .save(str(PATIENT_UTILIZATION_GOLD_PATH))
    )


def main() -> None:
    spark = create_spark_session("patient-utilization-gold")

    try:
        patients = read_silver_patients(spark)
        encounters = read_silver_encounters(spark)

        encounter_summary = summarize_encounters(encounters)

        patient_utilization = build_patient_utilization(
            patients,
            encounter_summary,
        )

        patient_utilization.select(
            "patient_id",
            "birth_date",
            "gender",
            "encounter_count",
            "first_encounter_at",
            "latest_encounter_at",
            "average_encounter_duration_minutes",
        ).show(truncate=False)

        print(
            f"Patient utilization rows: "
            f"{patient_utilization.count()}"
        )

        write_patient_utilization(patient_utilization)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()