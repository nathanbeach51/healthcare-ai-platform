import streamlit as st

from dashboard.data_loader import load_patient_observations


WEIGHT_CODE = "29463-7"
BMI_CODE = "39156-5"
SYSTOLIC_CODE = "8480-6"
DIASTOLIC_CODE = "8462-4"


def show_patient_explorer(
    patient_utilization,
    patient_conditions,
    patient_vitals,
    patient_medications,
    format_value,
    format_blood_pressure,
):
    st.header("Patient Explorer")

    selected_patient = st.selectbox(
        "Select Patient",
        sorted(
            patient_utilization["patient_id"].unique()
        ),
    )

    # --------------------------------------------------
    # Filter Gold tables to selected patient
    # --------------------------------------------------

    utilization_row = patient_utilization[
        patient_utilization["patient_id"] == selected_patient
    ]

    conditions_row = patient_conditions[
        patient_conditions["patient_id"] == selected_patient
    ]

    vitals_row = patient_vitals[
        patient_vitals["patient_id"] == selected_patient
    ]

    medications_row = patient_medications[
        patient_medications["patient_id"] == selected_patient
    ]

    utilization = utilization_row.iloc[0]
    conditions = conditions_row.iloc[0]
    vitals = vitals_row.iloc[0]
    medications = medications_row.iloc[0]

    # --------------------------------------------------
    # Patient Overview
    # --------------------------------------------------

    st.subheader("Patient Overview")

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Patient ID",
        selected_patient,
    )

    col2.metric(
        "Gender",
        str(utilization["gender"]).title(),
    )

    col3.metric(
        "Birth Date",
        str(utilization["birth_date"]),
    )

    col4.metric(
        "Encounters",
        f"{utilization['encounter_count']:,.0f}",
    )

    col1, col2, col3 = st.columns(3)

    col1.metric(
        "First Encounter",
        str(utilization["first_encounter_at"])[:10],
    )

    col2.metric(
        "Latest Encounter",
        str(utilization["latest_encounter_at"])[:10],
    )

    col3.metric(
        "Avg Encounter Duration",
        format_value(
            utilization["average_encounter_duration_minutes"],
            "{:.0f} min",
        ),
    )

    # --------------------------------------------------
    # Latest Vitals
    # --------------------------------------------------

    st.subheader("Latest Vitals")

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Weight",
        format_value(
            vitals["weight_kg"],
            "{:.1f} kg",
        ),
    )

    col2.metric(
        "BMI",
        format_value(
            vitals["bmi"],
            "{:.1f}",
        ),
    )

    col3.metric(
        "Blood Pressure",
        format_blood_pressure(
            vitals["systolic_bp"],
            vitals["diastolic_bp"],
        ),
    )

    col4.metric(
        "Heart Rate",
        format_value(
            vitals["heart_rate"],
            "{:.0f} bpm",
        ),
    )

    col1, col2 = st.columns(2)

    col1.metric(
        "Height",
        format_value(
            vitals["height_cm"],
            "{:.1f} cm",
        ),
    )

    col2.metric(
        "Respiratory Rate",
        format_value(
            vitals["respiratory_rate"],
            "{:.0f} /min",
        ),
    )

    # --------------------------------------------------
    # Weight Trend
    # --------------------------------------------------

    st.subheader("Weight Trend")

    weight_history = load_patient_observations(
        selected_patient,
        (WEIGHT_CODE,),
    )

    if not weight_history.empty:
        weight_chart = (
            weight_history[
                [
                    "observation_at",
                    "value_numeric",
                ]
            ]
            .set_index("observation_at")
            .rename(
                columns={
                    "value_numeric": "Weight (kg)",
                }
            )
        )

        st.line_chart(weight_chart)
    else:
        st.write("No weight history found.")

    # --------------------------------------------------
    # BMI Trend
    # --------------------------------------------------

    st.subheader("BMI Trend")

    bmi_history = load_patient_observations(
        selected_patient,
        (BMI_CODE,),
    )

    if not bmi_history.empty:
        bmi_chart = (
            bmi_history[
                [
                    "observation_at",
                    "value_numeric",
                ]
            ]
            .set_index("observation_at")
            .rename(
                columns={
                    "value_numeric": "BMI",
                }
            )
        )

        st.line_chart(bmi_chart)
    else:
        st.write("No BMI history found.")

    # --------------------------------------------------
    # Blood Pressure Trend
    # --------------------------------------------------

    st.subheader("Blood Pressure Trend")

    bp_history = load_patient_observations(
        selected_patient,
        (
            SYSTOLIC_CODE,
            DIASTOLIC_CODE,
        ),
    )

    if not bp_history.empty:
        bp_chart = (
            bp_history
            .pivot(
                index="observation_at",
                columns="observation_code",
                values="value_numeric",
            )
            .rename(
                columns={
                    SYSTOLIC_CODE: "Systolic",
                    DIASTOLIC_CODE: "Diastolic",
                }
            )
            .sort_index()
        )

        st.line_chart(bp_chart)
    else:
        st.write("No blood pressure history found.")

    # --------------------------------------------------
    # Clinical Conditions
    # --------------------------------------------------

    st.subheader("Clinical Conditions")

    clinical_names = conditions[
        "clinical_condition_names"
    ]

    if len(clinical_names) > 0:
        for condition in clinical_names:
            st.write(f"• {condition}")
    else:
        st.write("No clinical conditions found.")

    # --------------------------------------------------
    # Social Factors
    # --------------------------------------------------

    st.subheader("Social Factors")

    social_names = conditions[
        "social_factor_names"
    ]

    if len(social_names) > 0:
        for factor in social_names:
            st.write(f"• {factor}")
    else:
        st.write("No social factors found.")

    # --------------------------------------------------
    # History
    # --------------------------------------------------

    st.subheader("History")

    history_names = conditions[
        "history_names"
    ]

    if len(history_names) > 0:
        for item in history_names:
            st.write(f"• {item}")
    else:
        st.write("No history items found.")

    # --------------------------------------------------
    # Behavioral Factors
    # --------------------------------------------------

    behavioral_names = conditions[
        "behavioral_factor_names"
    ]

    if len(behavioral_names) > 0:
        st.subheader("Behavioral Factors")

        for item in behavioral_names:
            st.write(f"• {item}")

    # --------------------------------------------------
    # Active Medications
    # --------------------------------------------------

    st.subheader("Active Medications")

    col1, col2, col3 = st.columns(3)

    col1.metric(
        "Active",
        f"{medications['active_medication_count']:,.0f}",
    )

    col2.metric(
        "Unique Medications",
        f"{medications['unique_medication_count']:,.0f}",
    )

    col3.metric(
        "Medication Requests",
        f"{medications['medication_request_count']:,.0f}",
    )

    active_medications = medications[
        "active_medication_names"
    ]

    if len(active_medications) > 0:
        for medication in active_medications:
            st.write(f"• {medication}")
    else:
        st.write("No active medications found.")