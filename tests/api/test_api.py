"""
Integration tests for the Healthcare AI Platform API.

Tests FastAPI endpoints against the locally running application and
underlying project data. AI-dependent endpoints are mocked separately
to avoid external API calls during automated testing.
"""

from unittest.mock import patch

from fastapi.testclient import TestClient

from api.main import app


client = TestClient(app)


def test_health():
    response = client.get("/health")

    assert response.status_code == 200

    body = response.json()

    assert body["status"] == "ok"
    assert body["service"] == "healthcare-ai-platform"


def test_patient_found():
    response = client.get(
        "/patients/187253"
    )

    assert response.status_code == 200

    body = response.json()

    assert body["patient_id"] == "187253"


def test_patient_not_found():
    response = client.get(
        "/patients/nonexistent-patient"
    )

    assert response.status_code == 404

    body = response.json()

    assert body["detail"] == "Patient not found"


@patch("api.routes.patients.ask_patient_ai")
def test_ask_patient(mock_ask_patient):
    mock_ask_patient.return_value = {
        "answer": "Loratadine was documented for allergy symptoms.",
        "clinical_notes": [
            {
                "document_reference_id": "test-document-123",
                "document_date": "2026-09-06T21:00:00",
                "section_name": "Assessment and Plan",
                "similarity": 0.91,
            }
        ],
    }

    response = client.post(
        "/patients/186886/ask",
        json={
            "question": (
                "What treatment was documented "
                "for allergies?"
            )
        },
    )

    assert response.status_code == 200

    body = response.json()

    assert body["patient_id"] == "186886"
    assert body["answer"] == (
        "Loratadine was documented for allergy symptoms."
    )

    assert len(body["sources"]) == 1

    assert (
        body["sources"][0]["document_reference_id"]
        == "test-document-123"
    )

    mock_ask_patient.assert_called_once_with(
    patient_id="186886",
    question=(
        "What treatment was documented "
        "for allergies?"
    ),
)

def test_patient_vitals():
    response = client.get(
        "/patients/187253/vitals"
    )

    assert response.status_code == 200

    body = response.json()

    assert body["patient_id"] == "187253"
    assert "systolic_bp" in body
    assert "diastolic_bp" in body

def test_patient_conditions():
    response = client.get(
        "/patients/187253/conditions"
    )

    assert response.status_code == 200

    body = response.json()

    assert body["patient_id"] == "187253"
    assert body["condition_count"] == 14
    assert (
        body["active_clinical_condition_count"]
        == 0
    )
    assert (
        body["source"]
        == "gold.patient_conditions"
    )

def test_patient_medications():
    response = client.get(
        "/patients/187253/medications"
    )

    assert response.status_code == 200

    body = response.json()

    assert body["patient_id"] == "187253"
    assert body["medication_request_count"] == 4
    assert body["unique_medication_count"] == 1
    assert body["active_medication_count"] == 0
    assert (
        body["source"]
        == "gold.patient_medications"
    )

def test_patient_utilization():
    response = client.get(
        "/patients/187253/utilization"
    )

    assert response.status_code == 200

    body = response.json()

    assert body["patient_id"] == "187253"
    assert body["encounter_count"] == 20
    assert (
        body["source"]
        == "gold.patient_utilization"
    )