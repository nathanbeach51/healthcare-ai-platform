"""
Create a scaled Observation dataset for Spark performance testing.

The production Bronze dataset is never modified. Existing FHIR
Observation resources are replicated with unique resource IDs and
written to a separate Delta table used only for M19 benchmarks.
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
    / "delta"
    / "bronze"
    / "observation"
)

PERFORMANCE_PATH = (
    PROJECT_ROOT
    / "data"
    / "performance"
    / "observation_5m"
)

SCALE_FACTOR = 64


def main() -> None:
    spark = create_spark_session(
        "create-scaled-observations"
    )

    try:
        bronze = (
            spark.read
            .format("delta")
            .load(str(SOURCE_PATH))
        )

        scaled = (
            bronze
            .crossJoin(
                spark.range(SCALE_FACTOR)
                .withColumnRenamed(
                    "id",
                    "_scale_id",
                )
            )
        )

        scaled = scaled.withColumn(
            "resource",
            F.col("resource").withField(
                "id",
                F.concat(
                    F.col("resource.id"),
                    F.lit("-perf-"),
                    F.col("_scale_id").cast("string"),
                ),
            ),

        )

        scaled = scaled.drop(
            "_scale_id"
        )

        print(
            f"Scaled rows: {scaled.count():,}"
        )

        (
            scaled.write
            .format("delta")
            .mode("overwrite")
            .save(str(PERFORMANCE_PATH))
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()  