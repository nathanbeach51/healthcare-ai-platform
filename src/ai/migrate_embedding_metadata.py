from pathlib import Path

from pyspark.sql import functions as F

from processing.spark_session import create_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[2]

CHUNK_PATH = (
    PROJECT_ROOT
    / "data"
    / "delta"
    / "ai"
    / "clinical_note_chunks"
)

EMBEDDING_PATH = (
    PROJECT_ROOT
    / "data"
    / "delta"
    / "ai"
    / "clinical_note_embeddings"
)

TEMP_PATH = (
    PROJECT_ROOT
    / "data"
    / "delta"
    / "ai"
    / "clinical_note_embeddings_migrated"
)


def main() -> None:

    spark = create_spark_session(
        "migrate-embedding-metadata"
    )

    try:
        embeddings = (
            spark.read
            .format("delta")
            .load(str(EMBEDDING_PATH))
        )

        chunks = (
            spark.read
            .format("delta")
            .load(str(CHUNK_PATH))
            .select(
                "chunk_id",
                "chunk_processed_at",
            )
        )

        print(
            "Existing embeddings:",
            embeddings.count(),
        )

        print(
            "Existing chunks:",
            chunks.count(),
        )

        migrated = (
            embeddings
            .join(
                chunks,
                on="chunk_id",
                how="left",
            )
            .withColumnRenamed(
                "chunk_processed_at",
                "source_chunk_processed_at",
            )
            .withColumn(
                "embedded_at",
                F.current_timestamp(),
            )
        )

        migrated_count = migrated.count()

        missing_metadata_count = (
            migrated
            .filter(
                F.col(
                    "source_chunk_processed_at"
                ).isNull()
            )
            .count()
        )

        print(
            "Migrated rows:",
            migrated_count,
        )

        print(
            "Rows missing chunk metadata:",
            missing_metadata_count,
        )

        if missing_metadata_count > 0:
            raise ValueError(
                "Some embeddings could not be "
                "matched to clinical note chunks."
            )

        (
            migrated.write
            .format("delta")
            .mode("overwrite")
            .option(
                "overwriteSchema",
                "true",
            )
            .save(str(TEMP_PATH))
        )

        print(
            "Migration written successfully "
            "to temporary Delta table."
        )

        verification = (
            spark.read
            .format("delta")
            .load(str(TEMP_PATH))
        )

        print(
            "Temporary table row count:",
            verification.count(),
        )

        verification.printSchema()

        print(
            "\nMigration validation complete."
        )
        print(
            "The original embedding table "
            "has NOT been modified yet."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()