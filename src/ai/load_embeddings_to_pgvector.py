from pathlib import Path
import os

import psycopg
from pgvector import Vector
from pgvector.psycopg import register_vector
from pyspark.sql import functions as F

from processing.spark_session import create_spark_session

import os

PGVECTOR_HOST = os.getenv(
    "PGVECTOR_HOST",
    "localhost",
)


PROJECT_ROOT = Path(__file__).resolve().parents[2]

EMBEDDING_PATH = (
    PROJECT_ROOT
    / "data"
    / "delta"
    / "ai"
    / "clinical_note_embeddings"
)

PIPELINE_NAME = "clinical_note_pgvector"
BATCH_SIZE = 500



PGVECTOR_HOST = os.getenv(
    "PGVECTOR_HOST",
    "localhost",
)


def get_connection():
    conn = psycopg.connect(
        host=PGVECTOR_HOST,
        port=5432,
        dbname="hapi",
        user="admin",
        password="admin",
    )

    register_vector(conn)

    return conn


def get_checkpoint(
    conn,
):
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT last_processed_at
            FROM pipeline_checkpoints
            WHERE pipeline_name = %s
            """,
            (PIPELINE_NAME,),
        )

        row = cursor.fetchone()

    if row is None:
        return None

    return row[0]


def save_checkpoint(
    conn,
    checkpoint,
) -> None:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            INSERT INTO pipeline_checkpoints (
                pipeline_name,
                last_processed_at
            )
            VALUES (%s, %s)
            ON CONFLICT (pipeline_name)
            DO UPDATE SET
                last_processed_at =
                    EXCLUDED.last_processed_at
            """,
            (
                PIPELINE_NAME,
                checkpoint,
            ),
        )

    conn.commit()


def get_pgvector_count(
    conn,
) -> int:
    with conn.cursor() as cursor:
        cursor.execute(
            """
            SELECT COUNT(*)
            FROM clinical_note_embeddings
            """
        )

        row = cursor.fetchone()

    return row[0]


def bootstrap_checkpoint(
    spark,
    conn,
    embeddings,
) -> bool:
    """
    If pgvector already contains the same number
    of rows as Delta, initialize the checkpoint
    without reloading every embedding.

    Returns True if checkpoint was bootstrapped.
    """

    delta_count = embeddings.count()

    pgvector_count = get_pgvector_count(
        conn
    )

    print(
        f"Delta embedding rows: "
        f"{delta_count}"
    )

    print(
        f"pgvector rows: "
        f"{pgvector_count}"
    )

    if delta_count != pgvector_count:
        return False

    latest_embedded_at = (
        embeddings
        .agg(
            F.max(
                "embedded_at"
            ).alias(
                "latest_embedded_at"
            )
        )
        .first()
        ["latest_embedded_at"]
    )

    if latest_embedded_at is None:
        return False

    save_checkpoint(
        conn,
        latest_embedded_at,
    )

    print(
        "Existing Delta and pgvector "
        "row counts match."
    )

    print(
        "Initialized pgvector checkpoint "
        f"to {latest_embedded_at}."
    )

    return True


def upsert_batch(
    conn,
    batch,
) -> int:
    if not batch:
        return 0

    values = []

    for row in batch:
        values.append(
            (
                row["chunk_id"],
                row[
                    "document_reference_id"
                ],
                row["patient_id"],
                row["encounter_id"],
                row["document_date"],
                row["section_name"],
                row["chunk_index"],
                row["chunk_text"],
                Vector(
                    row["embedding"]
                ),
                row["embedding_model"],
            )
        )

    with conn.cursor() as cursor:
        cursor.executemany(
            """
            INSERT INTO
                clinical_note_embeddings (
                    chunk_id,
                    document_reference_id,
                    patient_id,
                    encounter_id,
                    document_date,
                    section_name,
                    chunk_index,
                    chunk_text,
                    embedding,
                    embedding_model
                )
            VALUES (
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s,
                %s
            )
            ON CONFLICT (chunk_id)
            DO UPDATE SET
                document_reference_id =
                    EXCLUDED.document_reference_id,
                patient_id =
                    EXCLUDED.patient_id,
                encounter_id =
                    EXCLUDED.encounter_id,
                document_date =
                    EXCLUDED.document_date,
                section_name =
                    EXCLUDED.section_name,
                chunk_index =
                    EXCLUDED.chunk_index,
                chunk_text =
                    EXCLUDED.chunk_text,
                embedding =
                    EXCLUDED.embedding,
                embedding_model =
                    EXCLUDED.embedding_model
            """,
            values,
        )

    conn.commit()

    return len(values)


def load_embeddings(
    embeddings,
    conn,
) -> int:
    batch = []
    loaded_count = 0

    for row in embeddings.toLocalIterator():

        batch.append(row)

        if len(batch) >= BATCH_SIZE:
            loaded_count += upsert_batch(
                conn,
                batch,
            )

            print(
                f"Loaded {loaded_count} "
                "embeddings into pgvector."
            )

            batch = []

    if batch:
        loaded_count += upsert_batch(
            conn,
            batch,
        )

        print(
            f"Loaded {loaded_count} "
            "embeddings into pgvector."
        )

    return loaded_count


def main() -> None:
    spark = create_spark_session(
        "load-embeddings-to-pgvector"
    )

    conn = get_connection()

    try:
        embeddings = (
            spark.read
            .format("delta")
            .load(
                str(EMBEDDING_PATH)
            )
        )

        checkpoint = get_checkpoint(
            conn
        )

        print(
            "Last pgvector checkpoint:",
            checkpoint,
        )

        #
        # One-time bootstrap for the existing
        # fully-loaded pgvector table.
        #
        if checkpoint is None:
            bootstrapped = (
                bootstrap_checkpoint(
                    spark,
                    conn,
                    embeddings,
                )
            )

            if bootstrapped:
                print(
                    "No pgvector reload "
                    "required."
                )
                return

        #
        # Normal incremental processing.
        #
        if checkpoint is not None:
            embeddings = (
                embeddings
                .filter(
                    F.col("embedded_at")
                    > checkpoint
                )
            )

        new_count = embeddings.count()

        print(
            f"New/changed embeddings: "
            f"{new_count}"
        )

        if new_count == 0:
            print(
                "No embeddings to load "
                "into pgvector."
            )
            return

        #
        # Capture the high-water mark from
        # exactly the data being processed.
        #
        new_checkpoint = (
            embeddings
            .agg(
                F.max(
                    "embedded_at"
                ).alias(
                    "latest_embedded_at"
                )
            )
            .first()
            ["latest_embedded_at"]
        )

        loaded_count = load_embeddings(
            embeddings,
            conn,
        )

        #
        # Only advance the checkpoint after
        # every pgvector upsert succeeds.
        #
        save_checkpoint(
            conn,
            new_checkpoint,
        )

        print(
            "pgvector load complete."
        )

        print(
            f"Embeddings loaded: "
            f"{loaded_count}"
        )

        print(
            "Checkpoint advanced to:",
            new_checkpoint,
        )

    finally:
        conn.close()
        spark.stop()


if __name__ == "__main__":
    main()