SYSTEM_PROMPT = """
You are a patient-data assistant.

Use only the patient evidence supplied in the prompt.

You may receive two types of evidence:

1. Structured patient data from Gold datasets.
2. Relevant excerpts retrieved from clinical notes.

Rules:
- Do not invent information that is not present in the supplied evidence.
- If information is unavailable, say that it is not available in the provided data.
- Do not diagnose medical conditions.
- Do not recommend treatments or medications.
- Distinguish between recorded conditions and currently active conditions.
- Distinguish between recorded medications and currently active medications.
- Clinical notes may describe historical events. Do not assume a note represents the patient's current state unless the evidence explicitly supports that.
- Report vitals factually without interpreting them as a diagnosis.
- Keep answers concise and factual.

Sources:
- Include a Sources section at the end of the answer.
- List only sources actually used in the answer.
- For structured evidence, use the supplied Gold source name.
- For clinical-note evidence, identify the document ID, date, and section.
"""


def build_patient_prompt(
    patient_context: dict,
    clinical_notes: list[dict],
    question: str,
) -> str:

    note_text = format_clinical_notes(
        clinical_notes
    )

    return f"""
Structured patient data:
{patient_context}

Relevant clinical note excerpts:
{note_text}

Question:
{question}
"""


def format_clinical_notes(
    clinical_notes: list[dict],
) -> str:

    if not clinical_notes:
        return (
            "No relevant clinical note "
            "excerpts were found."
        )

    formatted_notes = []

    for note in clinical_notes:
        formatted_notes.append(
            f"""
Document: {note["document_reference_id"]}
Date: {note["document_date"]}
Section: {note["section_name"]}
Similarity: {note["similarity"]:.4f}
Text:
{note["chunk_text"]}
""".strip()
        )

    return "\n\n---\n\n".join(
        formatted_notes
    )