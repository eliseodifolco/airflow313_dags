from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

default_args = {
    "owner": "eliseo.difolco",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="postgres_schema_discovery",
    default_args=default_args,
    schedule=None,  # instead of schedule_interval=None
    catchup=False,
    tags=["postgres", "discovery"],
) as dag:

    @task
    def list_all_schemas():
        hook = PostgresHook(postgres_conn_id="postgres_audimex")
        conn = hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
            ORDER BY schema_name;
        """)
        schemas = cursor.fetchall()

        print("Schemas in the database:")
        for schema in schemas:
            print(f" - {schema[0]}")

        cursor.close()

    list_all_schemas()
