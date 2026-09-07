# src/ai/test_similarity.py

from pathlib import Path
import math

from openai import OpenAI
from pyspark.sql import SparkSession

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

TOP_K = 5

client = OpenAI()


def create_embedding(
    text: str,
) -> list[float]:
    response = client.embeddings.create(
        model=EMBEDDING_MODEL,
        input=text,
    )

    return response.data[0].embedding


def cosine_similarity(
    vector_a: list[float],
    vector_b: list[float],
) -> float:

    dot_product = sum(
        a * b
        for a, b in zip(
            vector_a,
            vector_b,
        )
    )

    magnitude_a = math.sqrt(
        sum(
            value * value
            for value in vector_a
        )
    )

    magnitude_b = math.sqrt(
        sum(
            value * value
            for value in vector_b
        )
    )

    if magnitude_a == 0 or magnitude_b == 0:
        return 0.0

    return (
        dot_product
        / (magnitude_a * magnitude_b)
    )


def read_patient_embeddings(
    spark: SparkSession,
    patient_id: str,
):
    return (
        spark.read
        .format("delta")
        .load(str(EMBEDDING_PATH))
        .filter(
            f"patient_id = '{patient_id}'"
        )
    )


def main() -> None:
    spark = create_spark_session(
        "clinical-note-similarity-test"
    )

    try:
        patient_chunks = (
            read_patient_embeddings(
                spark,
                PATIENT_ID,
            )
        )

        rows = patient_chunks.collect()

        print("\nClinical Note Similarity Test")
        print("-----------------------------")
        print(f"Patient: {PATIENT_ID}")
        print(f"Question: {QUESTION}")
        print(
            f"Patient chunks: {len(rows)}"
        )

        question_embedding = (
            create_embedding(
                QUESTION
            )
        )

        scored_chunks = []

        for row in rows:
            score = cosine_similarity(
                question_embedding,
                row.embedding,
            )

            scored_chunks.append({
                "score": score,
                "chunk_id": row.chunk_id,
                "document_reference_id":
                    row.document_reference_id,
                "document_date":
                    row.document_date,
                "section_name":
                    row.section_name,
                "chunk_text":
                    row.chunk_text,
            })

        ranked_chunks = sorted(
            scored_chunks,
            key=lambda item: item["score"],
            reverse=True,
        )

        print(
            f"\nTop {TOP_K} Results"
        )
        print("--------------------")

        for rank, result in enumerate(
            ranked_chunks[:TOP_K],
            start=1,
        ):
            print(
                f"\n#{rank}"
            )

            print(
                f"Similarity: "
                f"{result['score']:.4f}"
            )

            print(
                f"Chunk: "
                f"{result['chunk_id']}"
            )

            print(
                f"Date: "
                f"{result['document_date']}"
            )

            print(
                f"Section: "
                f"{result['section_name']}"
            )

            print(
                "Text:"
            )

            print(
                result["chunk_text"]
            )

    finally:
        spark.stop()


if __name__ == "__main__":
    main()