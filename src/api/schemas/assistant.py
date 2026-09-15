"""
Pydantic models for patient AI assistant API requests and responses.

Defines the external API contract for patient questions, generated answers,
and clinical-note retrieval provenance returned by the hybrid RAG pipeline.

These models separate the public API response structure from the internal
representation used by the AI and retrieval layers.
"""

from datetime import datetime

from pydantic import BaseModel


class ClinicalNoteSource(BaseModel):
    source_type: str = "clinical_note"
    document_reference_id: str
    document_date: datetime | None = None
    section_name: str | None = None
    similarity: float | None = None


class PatientQuestionRequest(BaseModel):
    question: str


class PatientQuestionResponse(BaseModel):
    patient_id: str
    question: str
    answer: str
    sources: list[ClinicalNoteSource]