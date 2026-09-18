from unittest.mock import patch

from fastapi.testclient import TestClient

from api.main import app


client = TestClient(app)

TEST_PATIENT_ID = "test-patient-123"

TEST_PATIENT_CONTEXT = {
    "patient_id": TEST_PATIENT_ID,
    "demographics": {
        "birth_date": "1980-01-01",
        "gender": "male",
    },
    "latest_vitals": {
        "height_cm": 178.0,
        "weight_kg": 80.0,
        "bmi": 25.2,
        "heart_rate": 72.0,
        "respiratory_rate": 16.0,
        "systolic_bp": 120.0,
        "diastolic_bp": 80.0,
    },
    "conditions": {
        "condition_count": 14,
        "active_clinical_condition_count": 0,
        "source": "gold.patient_conditions",
    },
    "medications": {
    "medication_request_count": 4,
    "unique_medication_count": 1,
    "active_medication_count": 0,
    "active_medication_names": [],
    "has_active_medications": False,
    "source": "gold.patient_medications",
},
    "utilization": {
        "encounter_count": 20,
        "source": "gold.patient_utilization",
    },
}


def test_health():
    response = client.get("/health")

    assert response.status_code == 200

    body = response.json()

    assert body["status"] == "ok"
    assert body["service"] == "healthcare-ai-platform"


@patch("api.routes.patients.build_patient_context")
def test_patient_found(mock_build_patient_context):
    mock_build_patient_context.return_value = (
        TEST_PATIENT_CONTEXT
    )

    response = client.get(
        f"/patients/{TEST_PATIENT_ID}"
    )

    assert response.status_code == 200

    body = response.json()

    assert body["patient_id"] == TEST_PATIENT_ID

    mock_build_patient_context.assert_called_once_with(
        TEST_PATIENT_ID
    )


@patch("api.routes.patients.build_patient_context")
def test_patient_not_found(mock_build_patient_context):
    mock_build_patient_context.return_value = None

    response = client.get(
        "/patients/nonexistent-patient"
    )

    assert response.status_code == 404

    body = response.json()

    assert body["detail"] == "Patient not found"


@patch("api.routes.patients.ask_patient_ai")
def test_ask_patient(mock_ask_patient):
    mock_ask_patient.return_value = {
        "answer": (
            "Loratadine was documented for allergy symptoms."
        ),
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
        f"/patients/{TEST_PATIENT_ID}/ask",
        json={
            "question": (
                "What treatment was documented "
                "for allergies?"
            )
        },
    )

    assert response.status_code == 200

    body = response.json()

    assert body["patient_id"] == TEST_PATIENT_ID
    assert body["answer"] == (
        "Loratadine was documented for allergy symptoms."
    )
    assert len(body["sources"]) == 1
    assert (
        body["sources"][0]["document_reference_id"]
        == "test-document-123"
    )

    mock_ask_patient.assert_called_once_with(
        patient_id=TEST_PATIENT_ID,
        question=(
            "What treatment was documented "
            "for allergies?"
        ),
    )


@patch("api.routes.patients.build_patient_context")
def test_patient_vitals(mock_build_patient_context):
    mock_build_patient_context.return_value = (
        TEST_PATIENT_CONTEXT
    )

    response = client.get(
        f"/patients/{TEST_PATIENT_ID}/vitals"
    )

    assert response.status_code == 200

    body = response.json()

    assert body["patient_id"] == TEST_PATIENT_ID
    assert "systolic_bp" in body
    assert "diastolic_bp" in body


@patch("api.routes.patients.build_patient_context")
def test_patient_conditions(mock_build_patient_context):
    mock_build_patient_context.return_value = (
        TEST_PATIENT_CONTEXT
    )

    response = client.get(
        f"/patients/{TEST_PATIENT_ID}/conditions"
    )

    assert response.status_code == 200

    body = response.json()

    assert body["patient_id"] == TEST_PATIENT_ID
    assert body["condition_count"] == 14
    assert (
        body["active_clinical_condition_count"]
        == 0
    )
    assert (
        body["source"]
        == "gold.patient_conditions"
    )


@patch("api.routes.patients.build_patient_context")
def test_patient_medications(mock_build_patient_context):
    mock_build_patient_context.return_value = (
        TEST_PATIENT_CONTEXT
    )

    response = client.get(
        f"/patients/{TEST_PATIENT_ID}/medications"
    )

    assert response.status_code == 200

    body = response.json()

    assert body["patient_id"] == TEST_PATIENT_ID
    assert body["medication_request_count"] == 4
    assert body["unique_medication_count"] == 1
    assert body["active_medication_count"] == 0
    assert (
        body["source"]
        == "gold.patient_medications"
    )


@patch("api.routes.patients.build_patient_context")
def test_patient_utilization(mock_build_patient_context):
    mock_build_patient_context.return_value = (
        TEST_PATIENT_CONTEXT
    )

    response = client.get(
        f"/patients/{TEST_PATIENT_ID}/utilization"
    )

    assert response.status_code == 200

    body = response.json()

    assert body["patient_id"] == TEST_PATIENT_ID
    assert body["encounter_count"] == 20
    assert (
        body["source"]
        == "gold.patient_utilization"
    )