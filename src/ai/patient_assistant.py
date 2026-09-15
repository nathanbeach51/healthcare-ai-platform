"""
Hybrid patient question-answering service.

Combines structured patient data from Gold Delta tables with semantically
retrieved clinical-note chunks from pgvector. The combined evidence is
provided to the language model to generate grounded patient-level answers.

Structured sources:
    - gold.patient_latest_vitals
    - gold.patient_conditions
    - gold.patient_medications
    - gold.patient_utilization

Unstructured sources:
    - FHIR DocumentReference clinical notes
    - pgvector clinical-note embeddings

This module contains application logic only. API and UI layers should call
these functions rather than duplicating retrieval or prompting logic.
"""
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


def ask_patient_question_detailed(
    patient_id: str,
    question: str,
) -> dict:

    patient_context = build_patient_context(
        patient_id
    )

    if not patient_context:
        return {
            "answer": (
                f"No patient record was found "
                f"for patient_id {patient_id}."
            ),
            "clinical_notes": [],
        }

    clinical_notes = search_clinical_notes(
        patient_id=patient_id,
        question=question,
        top_k=5,
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

    return {
        "answer": response.output_text,
        "clinical_notes": clinical_notes,
    }


def ask_patient_question(
    patient_id: str,
    question: str,
) -> str:

    result = ask_patient_question_detailed(
        patient_id=patient_id,
        question=question,
    )

    return result["answer"]