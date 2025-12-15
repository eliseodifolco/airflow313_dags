from datetime import datetime

from airflow.sdk.dag import dag
from airflow.sdk.task import task


@task
def hello():
    print("Hello from Airflow 3.1.3 minimal DAG!")
    # This should appear in the task log if everything is wired correctly


@dag(
    dag_id="test_minimal_sdk",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["debug", "minimal"],
)
def minimal_dag():
    hello()


dag_instance = minimal_dag()
