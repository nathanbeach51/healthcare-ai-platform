from pathlib import Path
from datetime import datetime, timezone

from delta.tables import DeltaTable
from openai import OpenAI
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

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

EMBEDDING_MODEL = "text-embedding-3-small"
EMBEDDING_DIMENSIONS = 1536
BATCH_SIZE = 50

EMBEDDING_SCHEMA = T.StructType(
    [
        T.StructField(
            "chunk_id",
            T.StringType(),
            False,
        ),
        T.StructField(
            "document_reference_id",
            T.StringType(),
            False,
        ),
        T.StructField(
            "patient_id",
            T.StringType(),
            False,
        ),
        T.StructField(
            "encounter_id",
            T.StringType(),
            True,
        ),
        T.StructField(
            "document_date",
            T.TimestampType(),
            True,
        ),
        T.StructField(
            "section_name",
            T.StringType(),
            True,
        ),
        T.StructField(
            "chunk_index",
            T.IntegerType(),
            True,
        ),
        T.StructField(
            "chunk_text",
            T.StringType(),
            False,
        ),
        T.StructField(
            "embedding",
            T.ArrayType(
                T.FloatType()
            ),
            False,
        ),
        T.StructField(
            "embedding_model",
            T.StringType(),
            False,
        ),
        T.StructField(
            "source_chunk_processed_at",
            T.TimestampType(),
            True,
        ),
        T.StructField(
            "embedded_at",
            T.TimestampType(),
            False,
        ),
    ]
)


def get_last_embedded_at(
    spark: SparkSession,
):
    if not DeltaTable.isDeltaTable(
        spark,
        str(EMBEDDING_PATH),
    ):
        return None

    embeddings = (
        spark.read
        .format("delta")
        .load(str(EMBEDDING_PATH))
    )

    if (
        "source_chunk_processed_at"
        not in embeddings.columns
    ):
        print(
            "Existing embedding table does not "
            "contain incremental metadata. "
            "Running full migration pass."
        )
        return None

    return (
        embeddings
        .agg(
            F.max(
                "source_chunk_processed_at"
            ).alias(
                "max_processed_at"
            )
        )
        .first()
        ["max_processed_at"]
    )


def read_new_chunks(
    spark: SparkSession,
    last_embedded_at=None,
):
    chunks = (
        spark.read
        .format("delta")
        .load(str(CHUNK_PATH))
    )

    if last_embedded_at is not None:
        chunks = chunks.filter(
            F.col("chunk_processed_at")
            > last_embedded_at
        )

    return chunks


def create_embeddings(
    texts: list[str],
) -> list[list[float]]:

    client = OpenAI()

    response = client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=texts,
    )

    return [
        item.embedding
        for item in response.data
    ]


def write_embedding_batch(
    spark: SparkSession,
    rows: list[dict],
) -> None:

    if not rows:
        return

    batch_df = spark.createDataFrame(
        rows,
        schema=EMBEDDING_SCHEMA,
    )

    if not DeltaTable.isDeltaTable(
        spark,
        str(EMBEDDING_PATH),
    ):
        (
            batch_df.write
            .format("delta")
            .mode("overwrite")
            .save(str(EMBEDDING_PATH))
        )

        return

    existing_embeddings = (
        spark.read
        .format("delta")
        .load(str(EMBEDDING_PATH))
    )

    if (
        "source_chunk_processed_at"
        not in existing_embeddings.columns
    ):
        print(
            "Migrating embedding table "
            "to incremental schema."
        )

        (
            batch_df.write
            .format("delta")
            .mode("overwrite")
            .option(
                "overwriteSchema",
                "true",
            )
            .save(str(EMBEDDING_PATH))
        )

        return

    target = DeltaTable.forPath(
        spark,
        str(EMBEDDING_PATH),
    )

    (
        target.alias("target")
        .merge(
            batch_df.alias("source"),
            (
                "target.chunk_id = "
                "source.chunk_id"
            ),
        )
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )


def embed_chunks(
    spark: SparkSession,
    chunks,
) -> int:

    batch = []
    total_embedded = 0

    for row in chunks.toLocalIterator():

        batch.append(row)

        if len(batch) >= BATCH_SIZE:
            total_embedded += (
                process_batch(
                    spark,
                    batch,
                )
            )

            batch = []

    if batch:
        total_embedded += process_batch(
            spark,
            batch,
        )

    return total_embedded


def process_batch(
    spark: SparkSession,
    batch,
) -> int:

    texts = [
        row["chunk_text"]
        for row in batch
    ]

    embeddings = create_embeddings(
        texts
    )

    embedded_at = datetime.now(
        timezone.utc
    )

    output_rows = []

    for row, embedding in zip(
        batch,
        embeddings,
    ):
        output_rows.append(
            {
                "chunk_id":
                    row["chunk_id"],

                "document_reference_id":
                    row[
                        "document_reference_id"
                    ],

                "patient_id":
                    row["patient_id"],

                "encounter_id":
                    row["encounter_id"],

                "document_date":
                    row["document_date"],

                "section_name":
                    row["section_name"],

                "chunk_index":
                    row["chunk_index"],

                "chunk_text":
                    row["chunk_text"],

                "embedding":
                    [
                        float(value)
                        for value
                        in embedding
                    ],

                "embedding_model":
                    EMBEDDING_MODEL,

                "source_chunk_processed_at":
                    row[
                        "chunk_processed_at"
                    ],

                "embedded_at":
                    embedded_at,
            }
        )

    write_embedding_batch(
        spark,
        output_rows,
    )

    print(
        f"Embedded batch of "
        f"{len(output_rows)} chunks."
    )

    return len(output_rows)


def main() -> None:

    spark = create_spark_session(
        "embed-clinical-notes"
    )

    try:
        last_embedded_at = (
            get_last_embedded_at(
                spark
            )
        )

        print(
            "Last embedded chunk timestamp:",
            last_embedded_at,
        )

        chunks = read_new_chunks(
            spark,
            last_embedded_at,
        )

        chunk_count = chunks.count()

        print(
            f"New/changed chunks: "
            f"{chunk_count}"
        )

        if chunk_count == 0:
            print(
                "No clinical note chunks "
                "to embed."
            )
            return

        total_embedded = embed_chunks(
            spark,
            chunks,
        )

        print(
            f"Embedding complete. "
            f"{total_embedded} chunks embedded."
        )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()