"""
Patient routes for the Healthcare AI Platform API.

Exposes patient-level healthcare data and AI-assisted question answering.

Patient data is retrieved from the platform's curated Gold datasets.
AI questions are handled by the hybrid patient assistant, which combines
structured Gold data with semantically retrieved clinical-note evidence.

Routes in this module should remain focused on HTTP request/response
handling rather than duplicating data retrieval or AI application logic.
"""

from fastapi import APIRouter, HTTPException

from ai.patient_assistant import (
    ask_patient_question_detailed as ask_patient_ai,
)

from api.schemas.patient import (
    PatientConditionsResponse,
    PatientMedicationsResponse,
    PatientUtilizationResponse,
    PatientVitalsResponse,
)

from ai.patient_context import build_patient_context
from api.schemas.assistant import (
    ClinicalNoteSource,
    PatientQuestionRequest,
    PatientQuestionResponse,
)


router = APIRouter(
    prefix="/patients",
    tags=["patients"],
)


@router.get("/{patient_id}")
def get_patient(patient_id: str):
    patient = build_patient_context(
        patient_id
    )

    if not patient:
        raise HTTPException(
            status_code=404,
            detail="Patient not found",
        )

    return patient

@router.get(
    "/{patient_id}/vitals",
    response_model=PatientVitalsResponse,
)
def get_patient_vitals(
    patient_id: str,
):
    patient = build_patient_context(
        patient_id
    )

    if not patient:
        raise HTTPException(
            status_code=404,
            detail="Patient not found",
        )

    vitals = patient.get("latest_vitals")

    if not vitals:
        raise HTTPException(
            status_code=404,
            detail="Patient vitals not found",
        )

    return PatientVitalsResponse(
        patient_id=patient_id,
        birth_date=patient["demographics"].get(
            "birth_date"
        ),
        gender=patient["demographics"].get(
            "gender"
        ),
        height_cm=vitals.get("height_cm"),
        weight_kg=vitals.get("weight_kg"),
        bmi=vitals.get("bmi"),
        heart_rate=vitals.get("heart_rate"),
        respiratory_rate=vitals.get(
            "respiratory_rate"
        ),
        systolic_bp=vitals.get("systolic_bp"),
        diastolic_bp=vitals.get("diastolic_bp"),
    )

@router.get(
    "/{patient_id}/utilization",
    response_model=PatientUtilizationResponse,
)
def get_patient_utilization(
    patient_id: str,
):
    patient = build_patient_context(
        patient_id
    )

    if not patient:
        raise HTTPException(
            status_code=404,
            detail="Patient not found",
        )

    utilization = patient.get("utilization")

    if not utilization:
        raise HTTPException(
            status_code=404,
            detail="Patient utilization not found",
        )

    return PatientUtilizationResponse(
        patient_id=patient_id,
        **utilization,
    )

@router.get(
    "/{patient_id}/medications",
    response_model=PatientMedicationsResponse,
)
def get_patient_medications(
    patient_id: str,
):
    patient = build_patient_context(
        patient_id
    )

    if not patient:
        raise HTTPException(
            status_code=404,
            detail="Patient not found",
        )

    medications = patient.get("medications")

    if not medications:
        raise HTTPException(
            status_code=404,
            detail="Patient medications not found",
        )

    return PatientMedicationsResponse(
        patient_id=patient_id,
        **medications,
    )

@router.get(
    "/{patient_id}/conditions",
    response_model=PatientConditionsResponse,
)
def get_patient_conditions(
    patient_id: str,
):
    patient = build_patient_context(
        patient_id
    )

    if not patient:
        raise HTTPException(
            status_code=404,
            detail="Patient not found",
        )

    conditions = patient.get("conditions")

    if not conditions:
        raise HTTPException(
            status_code=404,
            detail="Patient conditions not found",
        )

    return PatientConditionsResponse(
        patient_id=patient_id,
        **conditions,
    )

@router.post(
    "/{patient_id}/ask",
    response_model=PatientQuestionResponse,
)
def ask_patient_question(
    patient_id: str,
    request: PatientQuestionRequest,
):
    result = ask_patient_ai(
        patient_id=patient_id,
        question=request.question,
    )

    sources = [
        ClinicalNoteSource(
            document_reference_id=note[
                "document_reference_id"
            ],
            document_date=note[
                "document_date"
            ],
            section_name=note[
                "section_name"
            ],
            similarity=note[
                "similarity"
            ],
        )
        for note in result["clinical_notes"]
    ]

    return PatientQuestionResponse(
        patient_id=patient_id,
        question=request.question,
        answer=result["answer"],
        sources=sources,
    )