# src/processing/silver/document_reference_transform.py

from pathlib import Path
import base64

from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql.window import Window

from processing.spark_session import create_spark_session

from pyspark.sql import DataFrame, SparkSession

from processing.delta_utils import (
    get_last_ingested_at,
    merge_delta,
)

from config.settings import DEBUG_LOGGING


PROJECT_ROOT = Path(__file__).resolve().parents[3]

BRONZE_PATH = (
    PROJECT_ROOT
    / "data"
    / "delta"
    / "bronze"
    / "documentreference"
)

SILVER_PATH = (
    PROJECT_ROOT
    / "data"
    / "delta"
    / "silver"
    / "document_reference"
)

@F.udf(returnType=T.StringType())
def decode_base64_text(
    encoded_value: str,
) -> str | None:
    if not encoded_value:
        return None

    try:
        decoded_bytes = base64.b64decode(
            encoded_value
        )

        return decoded_bytes.decode(
            "utf-8"
        )

    except Exception:
        return None

def read_bronze_document_references(
    spark: SparkSession,
    last_ingested_at=None,
) -> DataFrame:
    bronze = (
        spark.read
        .format("delta")
        .load(str(BRONZE_PATH))
    )

    if last_ingested_at is not None:
        bronze = bronze.filter(
            F.col("_ingested_at") > last_ingested_at
        )

    return bronze   

def transform_document_references(
    bronze: DataFrame,
) -> DataFrame:

    return (
        bronze
        .select(
            F.col("resource.id").alias(
                "document_reference_id"
            ),

            F.regexp_replace(
                F.col("resource.subject.reference"),
                "^Patient/",
                "",
            ).alias(
                "patient_id"
            ),

            F.regexp_replace(
                F.col("resource.context.encounter")[0]["reference"],
                "^Encounter/",
                "",
            ).alias(
                "encounter_id"
            ),

            F.col(
                "resource.status"
            ).alias(
                "document_status"
            ),

            F.col(
                "resource.category"
            )[0]["coding"][0]["code"].alias(
                "document_category"
            ),

            F.col(
                "resource.type"
            )["coding"][0]["code"].alias(
                "document_type_code"
            ),

            F.col(
                "resource.type"
            )["coding"][0]["display"].alias(
                "document_type_display"
            ),

            F.col(
                "resource.date"
            ).cast(
                "timestamp"
            ).alias(
                "document_date"
            ),

            F.col(
                "resource.content"
            )[0]["attachment"]["contentType"].alias(
                "content_type"
            ),

            F.col(
                "resource.content"
            )[0]["attachment"]["data"].alias(
                "encoded_content"
            ),

            F.col(
                "resource.meta.lastUpdated"
            ).cast(
                "timestamp"
            ).alias(
                "source_last_updated"
            ),

            F.col("_source_file").alias(
                "source_file"
            ),

           F.col("_ingested_at").alias(
                "ingested_at"
            ),
        )
        .withColumn(
            "clinical_note_text",
            decode_base64_text(
                F.col("encoded_content")
            ),
        )
        .drop(
            "encoded_content"
        )
    )

def add_document_reference_fields(
    documents: DataFrame,
) -> DataFrame:
    return (
        documents
        .withColumn(
            "silver_processed_at",
            F.current_timestamp(),
        )
    )

def deduplicate_document_references(
    documents: DataFrame,
) -> DataFrame:

    window_spec = (
        Window
        .partitionBy(
            "document_reference_id"
        )
        .orderBy(
            F.col(
                "source_last_updated"
            ).desc_nulls_last(),
            F.col(
                "ingested_at"
            ).desc(),
        )
    )

    return (
        documents
        .withColumn(
            "_row_number",
            F.row_number().over(
                window_spec
            ),
        )
        .filter(
            F.col("_row_number") == 1
        )
        .drop(
            "_row_number"
        )
    )

def validate_document_references(
    documents: DataFrame,
) -> None:
    duplicate_count = (
        documents
        .groupBy("document_reference_id")
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    null_document_id_count = (
        documents
        .filter(
            F.col(
                "document_reference_id"
            ).isNull()
        )
        .count()
    )

    null_patient_id_count = (
        documents
        .filter(
            F.col("patient_id").isNull()
        )
        .count()
    )

    null_note_text_count = (
        documents
        .filter(
            F.col(
                "clinical_note_text"
            ).isNull()
        )
        .count()
    )

    if duplicate_count:
        raise ValueError(
            f"Found {duplicate_count} "
            "duplicate document reference IDs."
        )

    if null_document_id_count:
        raise ValueError(
            f"Found {null_document_id_count} "
            "null document reference IDs."
        )

    if null_patient_id_count:
        raise ValueError(
            f"Found {null_patient_id_count} "
            "null patient IDs."
        )

    if null_note_text_count:
        raise ValueError(
            f"Found {null_note_text_count} "
            "documents with null clinical note text."
        )

def write_silver_document_references(
    spark: SparkSession,
    documents: DataFrame,
) -> None:
    merge_delta(
        spark=spark,
        source=documents,
        target_path=SILVER_PATH,
        merge_condition=(
            "target.document_reference_id = "
            "source.document_reference_id"
        ),
    )

def main() -> None:
    spark = create_spark_session(
        "document-reference-silver"
    )

    try:
        last_ingested_at = (
            get_last_ingested_at(
                spark,
                SILVER_PATH,
            )
        )

        bronze = (
            read_bronze_document_references(
                spark,
                last_ingested_at,
            )
        )

        bronze_count = bronze.count()

        print("\nDocumentReference Silver")
        print("------------------------")
        print(
            f"New Bronze rows: {bronze_count}"
        )

        if bronze_count == 0:
            print("Rows to merge: 0")
            print("Status: NO NEW DATA")
            return

        transformed = (
            transform_document_references(
                bronze
            )
        )

        current = (
            deduplicate_document_references(
                transformed
            )
        )

        silver = (
            add_document_reference_fields(
                current
            )
        )

        validate_document_references(
            silver
        )

        silver_count = silver.count()

        print(
            f"Rows to merge: {silver_count}"
        )

        if DEBUG_LOGGING:
            print(
                "\nSilver DocumentReference sample:"
            )

            silver.select(
                "document_reference_id",
                "patient_id",
                "document_date",
                "content_type",
                "clinical_note_text",
            ).show(
                10,
                truncate=False,
            )

        write_silver_document_references(
            spark,
            silver,
        )

        saved = (
            spark.read
            .format("delta")
            .load(str(SILVER_PATH))
        )

        print(
            f"Total Silver rows: "
            f"{saved.count()}"
        )

        print("Status: SUCCESS")

    finally:
        spark.stop()


if __name__ == "__main__":
    main()