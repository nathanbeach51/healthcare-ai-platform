"""
Create a patient-partitioned Observation Delta dataset for
Spark performance testing.

The source performance dataset is not modified. Patient ID is
extracted from the FHIR subject reference and used as a Delta
partition column.
"""

from pathlib import Path

from pyspark.sql import functions as F

from processing.spark_session import (
    create_spark_session,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]

SOURCE_PATH = (
    PROJECT_ROOT
    / "data"
    / "performance"
    / "observation_5m"
)

PARTITIONED_PATH = (
    PROJECT_ROOT
    / "data"
    / "performance"
    / "observation_5m_by_patient"
)


def main() -> None:
    spark = create_spark_session(
        "create-patient-partitioned-observations"
    )

    try:
        bronze = (
            spark.read
            .format("delta")
            .load(str(SOURCE_PATH))
        )

        partitioned = bronze.withColumn(
            "patient_id",
            F.regexp_extract(
                F.col("resource.subject.reference"),
                r"Patient/(.+)",
                1,
            ),
        )

        (
            partitioned.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("patient_id")
            .save(str(PARTITIONED_PATH))
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()