from datetime import datetime, timedelta

from airflow.sdk import DAG, task

from extract_resources import extract_resources
from processing.bronze_to_delta import (
    FHIR_RESOURCES,
    process_resources,
)

from processing.silver.patient_transform import main as patient_main
from processing.silver.encounter_transform import main as encounter_main
from processing.silver.condition_transform import main as condition_main
from processing.silver.observation_transform import main as observation_main
from processing.silver.medication_request_transform import main as medication_main
from processing.gold.patient_conditions import main as gold_patient_conditions_main
from processing.gold.patient_latest_vitals import main as gold_patient_latest_vitals_main
from processing.gold.patient_medications import main as gold_patient_meds_main
from processing.gold.patient_utilization import main as gold_patient_util_main


with DAG(
    dag_id="healthcare_pipeline",
    start_date=datetime(2026, 8, 23),
    schedule=None,
    catchup=False,
    tags=["healthcare-ai"],
) as dag:

    @task(
        retries=3,
        retry_delay=timedelta(seconds=30),
    )
    def extract_fhir_resources():
        extract_resources()

    @task
    def process_bronze_to_delta():
        process_resources(
            FHIR_RESOURCES
        )

    @task
    def process_patient_silver():
        patient_main()


    @task
    def process_encounter_silver():
        encounter_main()


    @task
    def process_condition_silver():
        condition_main()


    @task
    def process_observation_silver():
        observation_main()


    @task
    def process_medication_silver():
        medication_main()

    @task
    def validate_silver_pipeline():
        print("All Silver tasks completed successfully.")
        print("Silver pipeline validation passed.")

    @task
    def process_gold_patient_conditions():
        gold_patient_conditions_main()

    @task
    def process_gold_patient_latest_vitals():
        gold_patient_latest_vitals_main()

    @task
    def process_gold_patient_meds():
        gold_patient_meds_main()

    @task
    def process_gold_patient_util():
        gold_patient_util_main()

    extract_fhir = extract_fhir_resources()
    bronze = process_bronze_to_delta()
    patient = process_patient_silver()
    encounter = process_encounter_silver()
    condition = process_condition_silver()
    observation = process_observation_silver()
    medication = process_medication_silver()    
    validation = validate_silver_pipeline()
    gold_patient_conditions = process_gold_patient_conditions()
    gold_patient_latest_vitals = process_gold_patient_latest_vitals()
    gold_patient_meds = process_gold_patient_meds()
    gold_patient_util = process_gold_patient_util()

    extract_fhir >> bronze

    bronze >> [
    patient,
    encounter,
    condition,
    observation,
    medication,
]

[
    patient,
    encounter,
    condition,
    observation,
    medication,
] >> validation

validation >> [
    gold_patient_conditions,
    gold_patient_latest_vitals,
    gold_patient_meds,
    gold_patient_util,
]