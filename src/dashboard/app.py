import streamlit as st

import pandas as pd

from dashboard.views.population import show_population
from dashboard.views.patient import show_patient_explorer

from dashboard.data_loader import (
    load_patient_conditions,
    load_patient_latest_vitals,
    load_patient_medications,
    load_patient_utilization,
)


def format_value(
    value,
    format_string: str,
    fallback: str = "N/A",
) -> str:
    if pd.isna(value):
        return fallback

    return format_string.format(value)

def format_blood_pressure(
    systolic,
    diastolic,
) -> str:
    if pd.isna(systolic) or pd.isna(diastolic):
        return "N/A"

    return f"{systolic:.0f}/{diastolic:.0f}"

# --------------------------------------------------
# Load Gold data
# --------------------------------------------------

st.title("Healthcare Analytics Dashboard")

patient_utilization = load_patient_utilization()
patient_conditions = load_patient_conditions()
patient_vitals = load_patient_latest_vitals()
patient_medications = load_patient_medications()

population_tab, patient_tab = st.tabs(
    [
        "Population Analytics",
        "Patient Explorer",
    ]
)

# --------------------------------------------------
# Population Overview
# --------------------------------------------------

with population_tab:
    show_population(
        patient_utilization,
        patient_conditions,
    )



with patient_tab:
    show_patient_explorer(
        patient_utilization,
        patient_conditions,
        patient_vitals,
        patient_medications,
        format_value,
        format_blood_pressure,
    )