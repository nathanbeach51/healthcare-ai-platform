"""
Create a scaled Silver Observation dataset for M19 performance testing.

Uses the production Observation Silver transformations against the
scaled Bronze performance dataset without modifying production data.
"""

from pathlib import Path

from processing.spark_session import create_spark_session
from processing.silver.observation_transform import (
    transform_simple_observations,
    transform_component_observations,
    combine_observations,
    deduplicate_observations,
    add_silver_metadata,
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]

BRONZE_PERFORMANCE_PATH = (
    PROJECT_ROOT
    / "data"
    / "performance"
    / "observation_5m"
)

SILVER_PERFORMANCE_PATH = (
    PROJECT_ROOT
    / "data"
    / "performance"
    / "observation_silver_5m"
)


def main() -> None:
    spark = create_spark_session(
        "create-scaled-silver-observations"
    )

    try:
        bronze = (
            spark.read
            .format("delta")
            .load(str(BRONZE_PERFORMANCE_PATH))
        )

        simple = transform_simple_observations(
            bronze
        )

        components = transform_component_observations(
            bronze
        )

        combined = combine_observations(
            simple,
            components,
        )

        silver = deduplicate_observations(
            combined
        )

        silver = add_silver_metadata(
            silver
        )

        (
            silver.write
            .format("delta")
            .mode("overwrite")
            .save(str(SILVER_PERFORMANCE_PATH))
        )

        saved = (
            spark.read
            .format("delta")
            .load(str(SILVER_PERFORMANCE_PATH))
        )

        print("\nScaled Silver Observation")
        print("-------------------------")
        print(
            f"Silver rows: "
            f"{saved.count():,}"
        )
        print(
            f"Silver partitions: "
            f"{saved.rdd.getNumPartitions()}"
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()