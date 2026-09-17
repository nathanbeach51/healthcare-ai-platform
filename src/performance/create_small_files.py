from pathlib import Path

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

SMALL_FILE_PATH = (
    PROJECT_ROOT
    / "data"
    / "performance"
    / "observation_5m_small_files"
)


def main() -> None:
    spark = create_spark_session(
        "create-small-file-observations"
    )

    try:
        bronze = (
            spark.read
            .format("delta")
            .load(str(SOURCE_PATH))
        )

        small_file_bronze = bronze.repartition(200)

        (
            small_file_bronze.write
            .format("delta")
            .mode("overwrite")
            .save(str(SMALL_FILE_PATH))
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()