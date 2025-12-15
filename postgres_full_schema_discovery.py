from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

default_args = {
    "owner": "eliseo.difolco",
    "start_date": datetime(2024, 1, 1),
}

with DAG(
    dag_id="postgres_full_schema_discovery",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["postgres", "discovery"],
) as dag:

    @task
    def list_schemas():
        hook = PostgresHook(postgres_conn_id="postgres_audimex")
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT schema_name
            FROM information_schema.schemata
            WHERE schema_name NOT IN ('pg_catalog', 'information_schema')
            ORDER BY schema_name;
        """)
        schemas = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return schemas

    @task
    def list_tables(schema_name: str):
        hook = PostgresHook(postgres_conn_id="postgres_audimex")
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT table_name
            FROM information_schema.tables
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            ORDER BY table_name;
        """, (schema_name,))
        tables = [row[0] for row in cursor.fetchall()]
        cursor.close()
        return tables

    @task
    def introspect_table(schema_name: str, table_name: str):
        hook = PostgresHook(postgres_conn_id="postgres_audimex")
        conn = hook.get_conn()
        cursor = conn.cursor()
        try:
            cursor.execute(f'SELECT * FROM "{schema_name}"."{table_name}" LIMIT 1')
            metadata = cursor.description

            print(f"\nSchema: {schema_name}")
            print(f"Table: {table_name}")
            print("Columns:")
            for col in metadata:
                print(f" - {col.name} (type code: {col.type_code})")

        except Exception as e:
            print(f"[ERROR] {schema_name}.{table_name}: {e}")

        cursor.close()

    # ---- DYNAMIC TASK MAPPING ----

    schemas = list_schemas()

    tables_per_schema = list_tables.expand(schema_name=schemas)

    introspect_table.expand(
        schema_name=schemas,
        table_name=tables_per_schema
    )
