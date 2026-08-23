from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from processing.spark_session import create_spark_session

from delta.tables import DeltaTable

from processing.delta_utils import get_last_ingested_at, merge_delta

from config.settings import DEBUG_LOGGING


PROJECT_ROOT = Path(__file__).resolve().parents[3]

ENCOUNTER_BRONZE_DELTA_PATH = (
    PROJECT_ROOT / "data" / "delta" / "bronze" / "encounter"
)

ENCOUNTER_SILVER_DELTA_PATH = (
    PROJECT_ROOT / "data" / "delta" / "silver" / "encounter"
)

def read_bronze_encounters(
    spark: SparkSession,
    last_ingested_at=None,
) -> DataFrame:
    bronze = (
        spark.read
        .format("delta")
        .load(str(ENCOUNTER_BRONZE_DELTA_PATH))
    )

    if last_ingested_at is not None:
        bronze = bronze.filter(
            F.col("_ingested_at") > last_ingested_at
        )

    return bronze

def transform_encounters(
    bronze: DataFrame,
) -> DataFrame:
    return bronze.select(
        F.col("resource.id").alias("encounter_id"),

        F.regexp_extract(
            F.col("resource.subject.reference"),
            r"Patient/(.+)",
            1,
        ).alias("patient_id"),

        F.col("resource.status").alias("encounter_status"),

        F.col("resource.class.code")
            .alias("encounter_class"),

        F.col("resource.type")[0]["coding"][0]["code"]
            .alias("encounter_type_code"),

        F.col("resource.type")[0]["coding"][0]["display"]
            .alias("encounter_type_display"),

        F.col("resource.period.start")
            .cast("timestamp")
            .alias("encounter_start"),

        F.col("resource.period.end")
            .cast("timestamp")
            .alias("encounter_end"),

        F.col("resource.meta.lastUpdated")
            .cast("timestamp")
            .alias("source_last_updated"),

        F.col("_source_file").alias("source_file"),
        F.col("_ingested_at").alias("ingested_at"),
    )


def deduplicate_encounters(
    encounters: DataFrame,
) -> DataFrame:
    latest_encounter = (
        Window
        .partitionBy("encounter_id")
        .orderBy(
            F.col("source_last_updated").desc_nulls_last(),
            F.col("ingested_at").desc(),
        )
    )

    return (
        encounters
        .withColumn(
            "_row_number",
            F.row_number().over(latest_encounter),
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

def add_encounter_fields(
    encounters: DataFrame,
) -> DataFrame:
    return (
        encounters
        .withColumn(
            "encounter_duration_minutes",
            (
                F.unix_timestamp("encounter_end")
                - F.unix_timestamp("encounter_start")
            ) / 60,
        )
        .withColumn(
            "silver_processed_at",
            F.current_timestamp(),
        )
    )


def write_silver_encounters(
    spark: SparkSession,
    encounters: DataFrame,
) -> None:
    merge_delta(
        spark=spark,
        source=encounters,
        target_path=ENCOUNTER_SILVER_DELTA_PATH,
        merge_condition=(
            "target.encounter_id = source.encounter_id"
        ),
    )

def main() -> None:
    spark = create_spark_session("encounter-silver")

    try:
        last_ingested_at = get_last_ingested_at(
            spark,
            ENCOUNTER_SILVER_DELTA_PATH,
        )

        bronze = read_bronze_encounters(
            spark,
            last_ingested_at,
        )

        bronze_count = bronze.count()

        print("\nEncounter Silver")
        print("----------------")
        print(f"New Bronze rows: {bronze_count}")

        if bronze_count == 0:
            print("Rows to merge: 0")
            print("Status: NO NEW DATA")
            return

        transformed = transform_encounters(
            bronze
        )

        if DEBUG_LOGGING:
            print("\nTransformed Encounter sample:")
            transformed.show(
                5,
                truncate=False,
            )

        current_encounters = (
            deduplicate_encounters(
                transformed
            )
        )

        silver = add_encounter_fields(
            current_encounters
        )

        silver_count = silver.count()

        print(
            f"Rows to merge: {silver_count}"
        )

        if DEBUG_LOGGING:
            print("\nSilver Encounter sample:")
            silver.show(
                5,
                truncate=False,
            )

        write_silver_encounters(
            spark,
            silver,
        )

        saved = (
            spark.read
            .format("delta")
            .load(
                str(
                    ENCOUNTER_SILVER_DELTA_PATH
                )
            )
        )

        saved_count = saved.count()

        print(
            f"Total Silver rows: {saved_count}"
        )
        print("Status: SUCCESS")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()