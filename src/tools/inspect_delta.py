from pathlib import Path

from processing.spark_session import create_spark_session

from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[2]


def main():
    spark = create_spark_session("inspect-delta")

    try:
        path = (
            PROJECT_ROOT
            / "data"
            / "delta"
            / "bronze"
            / "condition"
        )

        df = (
            spark.read
            .format("delta")
            .load(str(path))
        )

        print(f"Rows: {df.count()}")

        df.printSchema()

        df.select(
    F.col("resource.code.coding")[0]["code"]
        .alias("condition_code"),

    F.col("resource.code.coding")[0]["display"]
        .alias("condition_display"),
).distinct().orderBy(
    "condition_display"
).show(
    500,
    truncate=False,
)

    finally:
        spark.stop()


if __name__ == "__main__":
    main()