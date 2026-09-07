from openai import OpenAI

from ai.patient_context import (
    build_patient_context,
)
from ai.clinical_note_retriever import (
    search_clinical_notes,
)
from ai.prompts import (
    SYSTEM_PROMPT,
    build_patient_prompt,
)


client = OpenAI()


def ask_patient_question(
    patient_id: str,
    question: str,
) -> str:

    patient_context = (
        build_patient_context(
            patient_id
        )
    )

    if not patient_context:
        return (
            f"No patient record was found "
            f"for patient_id {patient_id}."
        )

    clinical_notes = (
        search_clinical_notes(
            patient_id=patient_id,
            question=question,
            top_k=5,
        )
    )

    prompt = build_patient_prompt(
        patient_context=patient_context,
        clinical_notes=clinical_notes,
        question=question,
    )

    response = client.responses.create(
        model="gpt-5.6-luna",
        instructions=SYSTEM_PROMPT,
        input=prompt,
    )

    return response.output_text