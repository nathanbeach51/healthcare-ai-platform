from openai import OpenAI
import psycopg

from pgvector import Vector
from pgvector.psycopg import register_vector


EMBEDDING_MODEL = "text-embedding-3-small"

client = OpenAI()


def get_connection():
    conn = psycopg.connect(
        host="localhost",
        port=5432,
        dbname="hapi",
        user="admin",
        password="admin",
    )

    register_vector(conn)

    return conn


def create_question_embedding(
    question: str,
) -> list[float]:

    response = client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=question,
    )

    return response.data[0].embedding


def search_clinical_notes(
    patient_id: str,
    question: str,
    top_k: int = 5,
) -> list[dict]:

    question_embedding = (
        create_question_embedding(
            question
        )
    )

    conn = get_connection()

    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT
                    chunk_id,
                    document_reference_id,
                    encounter_id,
                    document_date,
                    section_name,
                    chunk_text,
                    1 - (
                        embedding <=> %s
                    ) AS similarity
                FROM clinical_note_embeddings
                WHERE patient_id = %s
                ORDER BY embedding <=> %s
                LIMIT %s
                """,
                (
                    Vector(question_embedding),
                    patient_id,
                    Vector(question_embedding),
                    top_k,
                ),
            )

            rows = cursor.fetchall()

        return [
            {
                "chunk_id": row[0],
                "document_reference_id": row[1],
                "encounter_id": row[2],
                "document_date": row[3],
                "section_name": row[4],
                "chunk_text": row[5],
                "similarity": row[6],
                "source": "clinical_note",
            }
            for row in rows
        ]

    finally:
        conn.close()