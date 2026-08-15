import streamlit as st


def show_population(
    patient_utilization,
    patient_conditions,
):
    st.header("Population Overview")

    patient_count = len(patient_utilization)

    encounter_count = (
        patient_utilization["encounter_count"].sum()
    )

    active_condition_count = (
        patient_conditions[
            "active_clinical_condition_count"
        ].sum()
    )

    avg_encounters = (
        patient_utilization["encounter_count"].mean()
    )

    col1, col2, col3, col4 = st.columns(4)

    col1.metric(
        "Patients",
        f"{patient_count:,}",
    )

    col2.metric(
        "Encounters",
        f"{encounter_count:,.0f}",
    )

    col3.metric(
        "Active Clinical Conditions",
        f"{active_condition_count:,.0f}",
    )

    col4.metric(
        "Avg Encounters / Patient",
        f"{avg_encounters:.1f}",
    )

    # Top Clinical Conditions

    st.subheader("Top Clinical Conditions")

    clinical_conditions = (
        patient_conditions[
            [
                "patient_id",
                "clinical_condition_names",
            ]
        ]
        .explode("clinical_condition_names")
        .dropna()
    )

    top_clinical_conditions = (
        clinical_conditions[
            "clinical_condition_names"
        ]
        .value_counts()
        .head(10)
    )

    st.bar_chart(top_clinical_conditions)

    # Social Factors

    st.subheader("Social Factors")

    social_factors = (
        patient_conditions[
            [
                "patient_id",
                "social_factor_names",
            ]
        ]
        .explode("social_factor_names")
        .dropna()
    )

    top_social_factors = (
        social_factors[
            "social_factor_names"
        ]
        .value_counts()
        .head(10)
    )

    st.bar_chart(top_social_factors)

    # Encounter Utilization

    st.subheader("Encounter Utilization")

    encounter_data = (
        patient_utilization[
            [
                "patient_id",
                "encounter_count",
            ]
        ]
        .sort_values(
            "encounter_count",
            ascending=False,
        )
    )

    st.bar_chart(
        encounter_data.set_index("patient_id")
    )