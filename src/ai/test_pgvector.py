from pathlib import Path

from openai import OpenAI
import psycopg
from pgvector.psycopg import register_vector
from pgvector import Vector

from processing.spark_session import create_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[2]

EMBEDDING_PATH = (
    PROJECT_ROOT
    / "data"
    / "delta"
    / "ai"
    / "clinical_note_embeddings"
)

EMBEDDING_MODEL = "text-embedding-3-small"

PATIENT_ID = "186886"

QUESTION = (
    "What treatment did the patient "
    "receive for allergies?"
)

client = OpenAI()


def create_question_embedding(
    question: str,
) -> list[float]:
    response = client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=question,
    )

    return response.data[0].embedding


def main() -> None:
    spark = create_spark_session(
        "pgvector-test"
    )

    try:
        # Only grab one patient's chunks
        rows = (
            spark.read
            .format("delta")
            .load(str(EMBEDDING_PATH))
            .filter(
                f"patient_id = '{PATIENT_ID}'"
            )
            .collect()
        )

        print(
            f"Loading {len(rows)} "
            f"chunks for patient {PATIENT_ID}"
        )

        conn = psycopg.connect(
            host="localhost",
            port=5432,
            dbname="hapi",
            user="admin",
            password="admin",
        )

        register_vector(conn)

        with conn.cursor() as cursor:

            # Clear this patient's test rows
            cursor.execute(
                """
                DELETE FROM clinical_note_embeddings
                WHERE patient_id = %s
                """,
                (PATIENT_ID,),
            )

            for row in rows:
                cursor.execute(
                    """
                    INSERT INTO clinical_note_embeddings (
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
                        %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s
                    )
                    ON CONFLICT (chunk_id)
                    DO UPDATE SET
                        embedding = EXCLUDED.embedding
                    """,
                    (
                        row.chunk_id,
                        row.document_reference_id,
                        row.patient_id,
                        row.encounter_id,
                        row.document_date,
                        row.section_name,
                        row.chunk_index,
                        row.chunk_text,
                        Vector(row.embedding),
                        row.embedding_model,
                    ),
                )

        conn.commit()

        question_embedding = (
            create_question_embedding(
                QUESTION
            )
        )

        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    chunk_id,
                    document_date,
                    section_name,
                    chunk_text,
                    1 - (
                        embedding <=> %s
                    ) AS similarity
                FROM clinical_note_embeddings
                WHERE patient_id = %s
                ORDER BY embedding <=> %s
                LIMIT 5
                """,
                (
                    Vector(question_embedding),
                    PATIENT_ID,
                    Vector(question_embedding),
                ),
            )

            results = cursor.fetchall()

        print("\nPGVector Similarity Test")
        print("------------------------")
        print(f"Question: {QUESTION}")

        for rank, row in enumerate(
            results,
            start=1,
        ):
            (
                chunk_id,
                document_date,
                section_name,
                chunk_text,
                similarity,
            ) = row

            print(f"\n#{rank}")
            print(
                f"Similarity: "
                f"{similarity:.4f}"
            )
            print(f"Chunk: {chunk_id}")
            print(f"Date: {document_date}")
            print(
                f"Section: "
                f"{section_name}"
            )
            print("Text:")
            print(chunk_text)

        conn.close()

    finally:
        spark.stop()


if __name__ == "__main__":
    main()