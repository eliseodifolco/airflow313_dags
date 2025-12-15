from datetime import datetime
from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

with DAG(
    dag_id="test_snowflake_key_pair_connection",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "snowflake"],
):

    @task
    def test_connection():
        hook = SnowflakeHook(snowflake_conn_id="Snowflake_Key_Pair_Connection")
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT CURRENT_USER(), CURRENT_ROLE(), CURRENT_ACCOUNT(), "
            "CURRENT_REGION(), CURRENT_VERSION();"
        )
        print(cursor.fetchone())

    test_connection()
