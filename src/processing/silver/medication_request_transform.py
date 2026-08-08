from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from processing.spark_session import create_spark_session
from pyspark.sql.window import Window


PROJECT_ROOT = Path(__file__).resolve().parents[3]

MEDICATION_BRONZE_DELTA_PATH = (
    PROJECT_ROOT / "data" / "delta" / "bronze" / "medicationrequest"
)

MEDICATION_SILVER_DELTA_PATH = (
    PROJECT_ROOT
    / "data"
    / "delta"
    / "silver"
    / "medicationrequest"
)


def read_bronze_medication_requests(
    spark: SparkSession,
) -> DataFrame:
    return (
        spark.read
        .format("delta")
        .load(str(MEDICATION_BRONZE_DELTA_PATH))
    )

def deduplicate_medication_requests(
    medications: DataFrame,
) -> DataFrame:
    latest_medication = (
        Window
        .partitionBy("medication_request_id")
        .orderBy(
            F.col("source_last_updated").desc_nulls_last(),
            F.col("ingested_at").desc(),
        )
    )

    return (
        medications
        .withColumn(
            "_row_number",
            F.row_number().over(latest_medication),
        )
        .filter(F.col("_row_number") == 1)
        .drop("_row_number")
    )

def normalize_medication_fields(
    medications: DataFrame,
) -> DataFrame:
    return (
        medications
        .withColumn(
            "medication_reference_id",
            F.when(
                F.col("medication_reference_id") == "",
                None,
            ).otherwise(
                F.col("medication_reference_id")
            ),
        )
        .withColumn(
            "requester_id",
            F.when(
                F.col("requester_id") == "",
                None,
            ).otherwise(
                F.col("requester_id")
            ),
        )
    )

def add_silver_metadata(
    medications: DataFrame,
) -> DataFrame:
    return medications.withColumn(
        "silver_processed_at",
        F.current_timestamp(),
    )

def validate_medication_requests(
    medications: DataFrame,
) -> None:
    duplicate_count = (
        medications
        .groupBy("medication_request_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    null_request_id_count = (
        medications
        .filter(
            F.col("medication_request_id").isNull()
        )
        .count()
    )

    null_patient_id_count = (
        medications
        .filter(
            F.col("patient_id").isNull()
        )
        .count()
    )

    if duplicate_count:
        raise ValueError(
            f"Found {duplicate_count} duplicate medication request IDs."
        )

    if null_request_id_count:
        raise ValueError(
            f"Found {null_request_id_count} null medication request IDs."
        )

    if null_patient_id_count:
        raise ValueError(
            f"Found {null_patient_id_count} null patient IDs."
        )


def write_silver_medication_requests(
    medications: DataFrame,
) -> None:
    (
        medications.write
        .format("delta")
        .mode("overwrite")
        .save(str(MEDICATION_SILVER_DELTA_PATH))
    )

def transform_medication_requests(
    bronze: DataFrame,
) -> DataFrame:
    return bronze.select(
        F.col("resource.id").alias("medication_request_id"),

        F.regexp_extract(
            F.col("resource.subject.reference"),
            r"Patient/(.+)",
            1,
        ).alias("patient_id"),

        F.regexp_extract(
            F.col("resource.encounter.reference"),
            r"Encounter/(.+)",
            1,
        ).alias("encounter_id"),

        F.col(
            "resource.medicationCodeableConcept.coding"
        )[0]["code"].alias("medication_code"),

        F.col(
            "resource.medicationCodeableConcept.coding"
        )[0]["display"].alias("medication_display"),

        F.regexp_extract(
            F.col("resource.medicationReference.reference"),
            r"Medication/(.+)",
            1,
        ).alias("medication_reference_id"),

        F.col("resource.status").alias("status"),

        F.col("resource.intent").alias("intent"),

        F.col("resource.authoredOn")
            .cast("timestamp")
            .alias("authored_at"),

        F.regexp_extract(
            F.col("resource.requester.reference"),
            r"Practitioner/(.+)",
            1,
        ).alias("requester_id"),

        F.col("resource.requester.display")
            .alias("requester_display"),

        F.col("resource.meta.lastUpdated")
            .cast("timestamp")
            .alias("source_last_updated"),

        F.col("_source_file").alias("source_file"),
        F.col("_ingested_at").alias("ingested_at"),
    )

def main() -> None:
    spark = create_spark_session(
        "medication-request-silver"
    )

    try:
        bronze = read_bronze_medication_requests(
            spark
        )

        transformed = transform_medication_requests(
            bronze
        )

        current = deduplicate_medication_requests(
            transformed
        )

        normalized = normalize_medication_fields(
            current
        )

        silver = add_silver_metadata(
            normalized
        )

        validate_medication_requests(
            silver
        )

        print(
            f"Bronze medication snapshots: "
            f"{bronze.count()}"
        )

        print(
            f"Silver medication requests: "
            f"{silver.count()}"
        )

        silver.select(
            "medication_request_id",
            "patient_id",
            "encounter_id",
            "medication_code",
            "medication_display",
            "medication_reference_id",
            "status",
            "intent",
            "authored_at",
            "requester_id",
            "requester_display",
        ).show(30, truncate=False)

        write_silver_medication_requests(
            silver
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()