from pathlib import Path

from pyspark.sql import functions as F

from processing.spark_session import create_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[2]

CONDITION_SILVER_PATH = (
    PROJECT_ROOT
    / "data"
    / "delta"
    / "silver"
    / "condition"
)

CONDITION_CLASSIFICATION_PATH = (
    PROJECT_ROOT
    / "data"
    / "reference"
    / "condition_classification.csv"
)


def main():
    spark = create_spark_session("inspect-unclassified-conditions")

    try:
        conditions = (
            spark.read
            .format("delta")
            .load(str(CONDITION_SILVER_PATH))
        )

        classification = (
            spark.read
            .option("header", True)
            .schema(
                "condition_code string, condition_group string"
            )
            .csv(str(CONDITION_CLASSIFICATION_PATH))
        )

        unclassified = (
            conditions
            .join(
                classification,
                on="condition_code",
                how="left",
            )
            .filter(
                F.col("condition_group").isNull()
            )
        )

        print(
            f"Unclassified condition rows: "
            f"{unclassified.count()}"
        )

        unclassified.select(
            "condition_code",
            "condition_display",
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