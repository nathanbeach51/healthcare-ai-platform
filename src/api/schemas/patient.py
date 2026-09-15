"""
Pydantic models for patient data API responses.

Defines response schemas for structured patient information exposed by
the FastAPI service, including vitals and other patient-level domains.
"""

from datetime import date, datetime

from pydantic import BaseModel


class PatientVitalsResponse(BaseModel):
    patient_id: str
    birth_date: date | None = None
    gender: str | None = None
    height_cm: float | None = None
    weight_kg: float | None = None
    bmi: float | None = None
    heart_rate: float | None = None
    respiratory_rate: float | None = None
    systolic_bp: float | None = None
    diastolic_bp: float | None = None

class PatientConditionsResponse(BaseModel):
    patient_id: str
    source: str
    condition_count: int
    active_clinical_condition_count: int
    clinical_condition_names: list[str]
    social_factor_count: int
    social_factor_names: list[str]
    history_count: int
    history_names: list[str]
    behavioral_factor_count: int
    behavioral_factor_names: list[str]
    first_condition_onset_at: datetime | None = None
    latest_condition_onset_at: datetime | None = None
    has_active_clinical_condition: bool

class PatientMedicationsResponse(BaseModel):
    patient_id: str
    source: str
    medication_request_count: int
    unique_medication_count: int
    active_medication_count: int
    active_medication_names: list[str]
    latest_medication_authored_at: datetime | None = None
    has_active_medications: bool

class PatientUtilizationResponse(BaseModel):
    patient_id: str
    source: str
    encounter_count: int
    first_encounter_at: datetime | None = None
    latest_encounter_at: datetime | None = None
    average_encounter_duration_minutes: float | None = None