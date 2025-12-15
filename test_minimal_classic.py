from datetime import datetime

from airflow.decorators import dag, task


@task
def hello():
    print("Hello from minimal DAG via classic decorators!")
    # This should appear in the task log if everything is wired correctly


@dag(
    dag_id="test_minimal_classic",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["debug", "minimal"],
)
def minimal_dag():
    hello()


dag_instance = minimal_dag()
