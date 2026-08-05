from pathlib import Path

from pyspark.sql import functions as F

from processing.spark_session import create_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[2]

PATIENT_PATH = (
    PROJECT_ROOT
    / "data"
    / "bronze"
    / "patient"
)

PATIENT_DELTA_PATH = (
    PROJECT_ROOT
    / "data"
    / "delta"
    / "bronze"
    / "patient"
)


def main() -> None:
    spark = create_spark_session("patient-bronze-to-delta")

    try:
        bundles = (
            spark.read
            .option("multiLine", True)
            .json(str(PATIENT_PATH / "**" / "bundle.json"))
            .withColumn("_source_file", F.input_file_name())
        )

        patient_entries = bundles.select(
            F.explode("entry").alias("entry"),
            "_source_file",
        )

        patients = patient_entries.select(
            F.col("entry.resource").alias("resource"),
            "_source_file",
        )

        patients_with_metadata = (
            patients
            .withColumn("_ingested_at", F.current_timestamp())
            .withColumn("_resource_type", F.lit("Patient"))
        )

        print(
            f"Patient snapshots found: "
            f"{patients_with_metadata.count()}"
        )

        (
            patients_with_metadata.write
            .format("delta")
            .mode("overwrite")
            .save(str(PATIENT_DELTA_PATH))
        )

        saved_patients = (
            spark.read
            .format("delta")
            .load(str(PATIENT_DELTA_PATH))
        )

        print(f"Delta row count: {saved_patients.count()}")

        saved_patients.select(
            "resource.id",
            "resource.resourceType",
            "resource.gender",
            "resource.birthDate",
            "_source_file",
            "_ingested_at",
        ).show(truncate=False)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()