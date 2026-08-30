from ai.patient_context import build_patient_context
from ai.prompts import build_patient_prompt


patient_id = "187253"

context = build_patient_context(
    patient_id
)

prompt = build_patient_prompt(
    context,
    "Does this patient have any active medications?"
)

print(prompt)