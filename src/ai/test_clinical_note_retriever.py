from ai.clinical_note_retriever import (
    search_clinical_notes,
)


PATIENT_ID = "186886"

QUESTION = (
    "What treatment did the patient "
    "receive for allergies?"
)


def main() -> None:
    results = search_clinical_notes(
        patient_id=PATIENT_ID,
        question=QUESTION,
        top_k=5,
    )

    for index, result in enumerate(
        results,
        start=1,
    ):
        print(f"\n#{index}")
        print(
            f"Similarity: "
            f"{result['similarity']:.4f}"
        )
        print(
            f"Chunk: "
            f"{result['chunk_id']}"
        )
        print(
            f"Section: "
            f"{result['section_name']}"
        )
        print("Text:")
        print(
            result["chunk_text"]
        )


if __name__ == "__main__":
    main()