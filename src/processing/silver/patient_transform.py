from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from processing.spark_session import create_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[3]

PATIENT_BRONZE_DELTA_PATH = (
    PROJECT_ROOT / "data" / "delta" / "bronze" / "patient"
)

PATIENT_SILVER_DELTA_PATH = (
    PROJECT_ROOT / "data" / "delta" / "silver" / "patient"
)


def read_bronze_patients(spark: SparkSession) -> DataFrame:
    return (
        spark.read
        .format("delta")
        .load(str(PATIENT_BRONZE_DELTA_PATH))
    )


def transform_patients(bronze: DataFrame) -> DataFrame:
    return bronze.select(
        F.col("resource.id").alias("patient_id"),
        F.to_date("resource.birthDate").alias("birth_date"),
        F.col("resource.gender").alias("gender"),
        F.col("resource.meta.lastUpdated")
            .cast("timestamp")
            .alias("source_last_updated"),
        F.col("_source_file").alias("source_file"),
        F.col("_ingested_at").alias("ingested_at"),
    )


def deduplicate_patients(patients: DataFrame) -> DataFrame:
    latest_patient = (
        Window
        .partitionBy("patient_id")
        .orderBy(
            F.col("source_last_updated").desc_nulls_last(),
            F.col("ingested_at").desc(),
        )
    )

    return (
        patients
        .withColumn(
            "_row_number",
            F.row_number().over(latest_patient),
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def add_silver_metadata(patients: DataFrame) -> DataFrame:
    return (
        patients
        .withColumn(
            "silver_processed_at",
            F.current_timestamp(),
        )
        .withColumn(
            "is_current",
            F.lit(True),
        )
    )


def validate_patients(patients: DataFrame) -> None:
    duplicate_count = (
        patients.groupBy("patient_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    null_id_count = (
        patients.filter(
            F.col("patient_id").isNull()
        ).count()
    )

    if duplicate_count:
        raise ValueError(
            f"Found {duplicate_count} duplicate patient IDs."
        )

    if null_id_count:
        raise ValueError(
            f"Found {null_id_count} null patient IDs."
        )


def write_silver_patients(patients: DataFrame) -> None:
    (
        patients.write
        .format("delta")
        .mode("overwrite")
        .save(str(PATIENT_SILVER_DELTA_PATH))
    )


def main() -> None:
    spark = create_spark_session("patient-silver")

    try:
        bronze = read_bronze_patients(spark)

        print(f"Bronze rows: {bronze.count()}")

        flattened = transform_patients(bronze)

        current_patients = deduplicate_patients(flattened)

        silver = add_silver_metadata(current_patients)

        validate_patients(silver)

        print(f"Silver rows: {silver.count()}")

        silver.show(truncate=False)

        write_silver_patients(silver)

        saved = (
            spark.read
            .format("delta")
            .load(str(PATIENT_SILVER_DELTA_PATH))
        )

        print(f"Saved Silver rows: {saved.count()}")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()