from datetime import datetime

from airflow.sdk import DAG, task


with DAG(
    dag_id="healthcare_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["healthcare-ai"],
) as dag:

    @task
    def hello():
        print("Healthcare AI Platform Airflow test successful!")

    hello()