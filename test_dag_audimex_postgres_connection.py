from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime

DAG_ID = "test_postgres_audimex"

with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "postgres"],
):

    @task
    def test_connection():
        hook = PostgresHook(postgres_conn_id="postgres_audimex")

        # This actually opens the connection
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("SELECT version();")
        result = cursor.fetchone()

        print("PostgreSQL connection OK")
        print("Server version:", result)

    test_connection()
