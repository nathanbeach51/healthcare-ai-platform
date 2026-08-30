from openai import OpenAI

from ai.patient_context import build_patient_context
from ai.prompts import SYSTEM_PROMPT, build_patient_prompt


client = OpenAI()


def ask_patient_question(
    patient_id: str,
    question: str,
) -> str:

    context = build_patient_context(patient_id)

    if not context:
        return (
            f"No patient record was found "
            f"for patient_id {patient_id}."
        )

    prompt = build_patient_prompt(
        context,
        question,
    )

    response = client.responses.create(
        model="gpt-5.6-luna",
        instructions=SYSTEM_PROMPT,
        input=prompt,
    )

    return response.output_text


def main():
    answer = ask_patient_question(
        patient_id="187253",
        question="Is this patient's blood pressure dangerous?",
    )

    print(answer)


if __name__ == "__main__":
    main()