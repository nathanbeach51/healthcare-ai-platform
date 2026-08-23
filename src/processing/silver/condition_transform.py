from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from processing.spark_session import create_spark_session

from processing.delta_utils import get_last_ingested_at, merge_delta

from config.settings import DEBUG_LOGGING


PROJECT_ROOT = Path(__file__).resolve().parents[3]

CONDITION_BRONZE_DELTA_PATH = (
    PROJECT_ROOT / "data" / "delta" / "bronze" / "condition"
)

CONDITION_SILVER_DELTA_PATH = (
    PROJECT_ROOT / "data" / "delta" / "silver" / "condition"
)

def read_bronze_conditions(
    spark: SparkSession,
    last_ingested_at=None,
) -> DataFrame:
    bronze = (
        spark.read
        .format("delta")
        .load(str(CONDITION_BRONZE_DELTA_PATH))
    )

    if last_ingested_at is not None:
        bronze = bronze.filter(
            F.col("_ingested_at") > last_ingested_at
        )

    return bronze


def transform_conditions(
    bronze: DataFrame,
) -> DataFrame:
    return bronze.select(
        F.col("resource.id").alias("condition_id"),

        F.regexp_extract(
            F.col("resource.subject.reference"),
            r"Patient/(.+)",
            1,
        ).alias("patient_id"),

        F.col("resource.clinicalStatus.coding")[0]["code"]
            .alias("clinical_status"),

        F.col("resource.verificationStatus.coding")[0]["code"]
            .alias("verification_status"),

        F.col("resource.code.coding")[0]["code"]
            .alias("condition_code"),

        F.col("resource.code.coding")[0]["display"]
            .alias("condition_display"),

        F.col("resource.onsetDateTime")
            .cast("timestamp")
            .alias("onset_at"),

        F.col("resource.recordedDate")
            .cast("timestamp")
            .alias("recorded_at"),

        F.col("resource.meta.lastUpdated")
            .cast("timestamp")
            .alias("source_last_updated"),

        F.col("_source_file").alias("source_file"),
        F.col("_ingested_at").alias("ingested_at"),
    )


def deduplicate_conditions(
    conditions: DataFrame,
) -> DataFrame:
    latest_condition = (
        Window
        .partitionBy("condition_id")
        .orderBy(
            F.col("source_last_updated").desc_nulls_last(),
            F.col("ingested_at").desc(),
        )
    )

    return (
        conditions
        .withColumn(
            "_row_number",
            F.row_number().over(latest_condition),
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )


def add_condition_fields(
    conditions: DataFrame,
) -> DataFrame:
    return (
        conditions
        .withColumn(
            "is_active",
            F.col("clinical_status") == "active",
        )
        .withColumn(
            "silver_processed_at",
            F.current_timestamp(),
        )
    )


def validate_conditions(
    conditions: DataFrame,
) -> None:
    duplicate_count = (
        conditions.groupBy("condition_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    null_condition_id_count = (
        conditions
        .filter(F.col("condition_id").isNull())
        .count()
    )

    null_patient_id_count = (
        conditions
        .filter(F.col("patient_id").isNull())
        .count()
    )

    if duplicate_count:
        raise ValueError(
            f"Found {duplicate_count} duplicate condition IDs."
        )

    if null_condition_id_count:
        raise ValueError(
            f"Found {null_condition_id_count} null condition IDs."
        )

    if null_patient_id_count:
        raise ValueError(
            f"Found {null_patient_id_count} null patient IDs."
        )

def write_silver_conditions(
    spark: SparkSession,
    conditions: DataFrame,
) -> None:
    merge_delta(
        spark=spark,
        source=conditions,
        target_path=CONDITION_SILVER_DELTA_PATH,
        merge_condition=(
            "target.condition_id = source.condition_id"
        ),
    )

def main() -> None:
    spark = create_spark_session("condition-silver")

    try:
        last_ingested_at = get_last_ingested_at(
            spark,
            CONDITION_SILVER_DELTA_PATH,
        )

        bronze = read_bronze_conditions(
            spark,
            last_ingested_at,
        )

        bronze_count = bronze.count()

        print("\nCondition Silver")
        print("----------------")
        print(f"New Bronze rows: {bronze_count}")

        if bronze_count == 0:
            print("Rows to merge: 0")
            print("Status: NO NEW DATA")
            return

        transformed = transform_conditions(
            bronze
        )

        current = deduplicate_conditions(
            transformed
        )

        silver = add_condition_fields(
            current
        )

        validate_conditions(
            silver
        )

        silver_count = silver.count()

        print(
            f"Rows to merge: {silver_count}"
        )

        if DEBUG_LOGGING:
            print("\nSilver Condition sample:")
            silver.show(
                10,
                truncate=False,
            )

        write_silver_conditions(
            spark,
            silver,
        )

        saved = (
            spark.read
            .format("delta")
            .load(
                str(
                    CONDITION_SILVER_DELTA_PATH
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