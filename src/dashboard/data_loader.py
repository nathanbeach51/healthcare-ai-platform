from pathlib import Path

import streamlit as st

from processing.spark_session import create_spark_session

from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[2]

PATIENT_UTILIZATION_PATH = (
    PROJECT_ROOT
    / "data"
    / "delta"
    / "gold"
    / "patient_utilization"
)

PATIENT_CONDITIONS_PATH = (
    PROJECT_ROOT
    / "data"
    / "delta"
    / "gold"
    / "patient_conditions"
)

PATIENT_LATEST_VITALS_PATH = (
    PROJECT_ROOT
    / "data"
    / "delta"
    / "gold"
    / "patient_latest_vitals"
)

PATIENT_MEDICATIONS_PATH = (
    PROJECT_ROOT
    / "data"
    / "delta"
    / "gold"
    / "patient_medications"
)

OBSERVATION_SILVER_PATH = (
    PROJECT_ROOT
    / "data"
    / "delta"
    / "silver"
    / "observation"
)


@st.cache_data
def load_patient_observations(
    patient_id: str,
    observation_codes: tuple[str, ...],
):
    spark = get_spark()

    df = (
        spark.read
        .format("delta")
        .load(str(OBSERVATION_SILVER_PATH))
        .filter(
            (F.col("patient_id") == patient_id)
            & F.col("observation_code").isin(
                list(observation_codes)
            )
        )
        .select(
            "patient_id",
            "observation_code",
            "observation_display",
            "observation_at",
            "value_numeric",
            "unit",
        )
        .orderBy("observation_at")
    )

    return df.toPandas()


@st.cache_resource
def get_spark():
    return create_spark_session("streamlit-dashboard")


def load_gold_table(path: Path):
    spark = get_spark()

    return (
        spark.read
        .format("delta")
        .load(str(path))
        .toPandas()
    )


@st.cache_data
def load_patient_utilization():
    return load_gold_table(PATIENT_UTILIZATION_PATH)


@st.cache_data
def load_patient_conditions():
    return load_gold_table(PATIENT_CONDITIONS_PATH)

@st.cache_data
def load_patient_latest_vitals():
    return load_gold_table(PATIENT_LATEST_VITALS_PATH)

@st.cache_data
def load_patient_medications():
    return load_gold_table(PATIENT_MEDICATIONS_PATH)