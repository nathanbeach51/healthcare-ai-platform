from pathlib import Path
from typing import Any

from pyspark.sql import functions as F

from processing.spark_session import create_spark_session


PROJECT_ROOT = Path(__file__).resolve().parents[2]

LATEST_VITALS_GOLD_PATH = (
    PROJECT_ROOT
    / "data"
    / "delta"
    / "gold"
    / "patient_latest_vitals"
)

PATIENT_CONDITIONS_GOLD_PATH = (
    PROJECT_ROOT
    / "data"
    / "delta"
    / "gold"
    / "patient_conditions"
)

PATIENT_MEDICATIONS_GOLD_PATH = (
    PROJECT_ROOT
    / "data"
    / "delta"
    / "gold"
    / "patient_medications"
)

PATIENT_UTILIZATION_GOLD_PATH = (
    PROJECT_ROOT
    / "data"
    / "delta"
    / "gold"
    / "patient_utilization"
)


def format_datetime(value: Any):
    if value is None:
        return None

    return value.isoformat()


def get_patient_row(
    spark,
    path: Path,
    patient_id: str,
):
    patient_df = (
        spark.read
        .format("delta")
        .load(str(path))
        .filter(
            F.col("patient_id") == patient_id
        )
    )

    rows = patient_df.limit(1).collect()

    if not rows:
        return None

    return rows[0].asDict(recursive=True)


def build_patient_context(
    patient_id: str,
) -> dict:

    spark = create_spark_session()

    vitals = get_patient_row(
        spark,
        LATEST_VITALS_GOLD_PATH,
        patient_id,
    )

    conditions = get_patient_row(
        spark,
        PATIENT_CONDITIONS_GOLD_PATH,
        patient_id,
    )

    medications = get_patient_row(
        spark,
        PATIENT_MEDICATIONS_GOLD_PATH,
        patient_id,
    )

    utilization = get_patient_row(
        spark,
        PATIENT_UTILIZATION_GOLD_PATH,
        patient_id,
    )

    if vitals is None:
        return {}

    conditions = conditions or {}
    medications = medications or {}
    utilization = utilization or {}

    context = {
        "patient_id": patient_id,

       "demographics": {
            "source": "gold.patient_latest_vitals",
            "birth_date": format_datetime(
            vitals.get("birth_date")
            ),
        "gender": vitals.get("gender"),
    },

        "latest_vitals": {
            "source": "gold.patient_latest_vitals",
            "height_cm": vitals.get(
                "height_cm"
            ),
            "weight_kg": vitals.get(
                "weight_kg"
            ),
            "bmi": vitals.get(
                "bmi"
            ),
            "heart_rate": vitals.get(
                "heart_rate"
            ),
            "respiratory_rate": vitals.get(
                "respiratory_rate"
            ) ,
            "blood_pressure": {
                "systolic": vitals.get(
                    "systolic_bp"
                ),
                "diastolic": vitals.get(
                    "diastolic_bp"
                ),
            },
        },

        "conditions": {
            "source": "gold.patient_conditions",
            "condition_count": conditions.get(
                "condition_count"
            ),
            "active_clinical_condition_count":
                conditions.get(
                    "active_clinical_condition_count"
                ),
            "clinical_condition_names":
                conditions.get(
                    "clinical_condition_names",
                    [],
                ),
            "social_factor_count":
                conditions.get(
                    "social_factor_count"
                ),
            "social_factor_names":
                conditions.get(
                    "social_factor_names",
                    [],
                ),
            "history_count":
                conditions.get(
                    "history_count"
                ),
            "history_names":
                conditions.get(
                    "history_names",
                    [],
                ),
            "behavioral_factor_count":
                conditions.get(
                    "behavioral_factor_count"
                ),
            "behavioral_factor_names":
                conditions.get(
                    "behavioral_factor_names",
                    [],
                ),
            "first_condition_onset_at":
                format_datetime(
                    conditions.get(
                        "first_condition_onset_at"
                    )
                ),
            "latest_condition_onset_at":
                format_datetime(
                    conditions.get(
                        "latest_condition_onset_at"
                    )
                ),
            "has_active_clinical_condition":
                conditions.get(
                    "has_active_clinical_condition"
                ),
        },

        "medications": {
            "source": "gold.patient_medications",
            "medication_request_count":
                medications.get(
                    "medication_request_count"
                ),
            "unique_medication_count":
                medications.get(
                    "unique_medication_count"
                ),
            "active_medication_count":
                medications.get(
                    "active_medication_count"
                ),
            "active_medication_names":
                medications.get(
                    "active_medication_names",
                    [],
                ),
            "latest_medication_authored_at":
                format_datetime(
                    medications.get(
                        "latest_medication_authored_at"
                    )
                ),
            "has_active_medications":
                medications.get(
                    "has_active_medications"
                ),
        },

        "utilization": {
            "source": "gold.patient_utilization",
            "encounter_count":
                utilization.get(
                    "encounter_count"
                ),
            "first_encounter_at":
                format_datetime(
                    utilization.get(
                        "first_encounter_at"
                    )
                ),
            "latest_encounter_at":
                format_datetime(
                    utilization.get(
                        "latest_encounter_at"
                    )
                ),
            "average_encounter_duration_minutes":
                utilization.get(
                    "average_encounter_duration_minutes"
                ),
        },
    }

    return context


def main():
    patient_id = "187253"

    context = build_patient_context(
        patient_id
    )

    print(context)


if __name__ == "__main__":
    main()