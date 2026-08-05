from pathlib import Path
from typing import List

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql import SparkSession

from processing.spark_session import create_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_PATH = PROJECT_ROOT / "data" / "bronze"
DELTA_BRONZE_PATH = PROJECT_ROOT / "data" / "delta" / "bronze"

FHIR_RESOURCES = [
    "Patient",
    "Encounter",
    "Condition",
    "Observation",
    "MedicationRequest",
]


def read_resource_bundles(
    spark: SparkSession,
    resource_type: str,
) -> DataFrame:
    input_path = (
        BRONZE_PATH
        / resource_type.lower()
        / "**"
        / "bundle.json"
    )

    return (
        spark.read
        .option("multiLine", True)
        .json(str(input_path))
        .withColumn("_source_file", F.input_file_name())
    )


def extract_resources(bundles: DataFrame) -> DataFrame:
    entries = bundles.select(
        F.explode("entry").alias("entry"),
        "_source_file",
    )

    return entries.select(
        F.col("entry.resource").alias("resource"),
        "_source_file",
    )


def add_ingestion_metadata(
    resources: DataFrame,
    resource_type: str,
) -> DataFrame:
    return (
        resources
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_resource_type", F.lit(resource_type))
    )


def write_delta(
    resources: DataFrame,
    resource_type: str,
) -> Path:
    output_path = (
        DELTA_BRONZE_PATH
        / resource_type.lower()
    )

    (
        resources.write
        .format("delta")
        .mode("overwrite")
        .save(str(output_path))
    )

    return output_path


def process_resource(
    spark: SparkSession,
    resource_type: str,
) -> None:
    print(f"Processing {resource_type}...")

    bundles = read_resource_bundles(
        spark,
        resource_type,
    )

    resources = extract_resources(bundles)

    resources_with_metadata = add_ingestion_metadata(
        resources,
        resource_type,
    )

    output_path = write_delta(
        resources_with_metadata,
        resource_type,
    )

    saved_resources = (
        spark.read
        .format("delta")
        .load(str(output_path))
    )

    print(
        f"{resource_type}: "
        f"{saved_resources.count()} rows written to {output_path}"
    )


def process_resources(
    resource_types: List[str],
) -> None:
    spark = create_spark_session(
        "fhir-bronze-to-delta"
    )

    try:
        for resource_type in resource_types:
            process_resource(
                spark,
                resource_type,
            )
    finally:
        spark.stop()


if __name__ == "__main__":
    process_resources(FHIR_RESOURCES)