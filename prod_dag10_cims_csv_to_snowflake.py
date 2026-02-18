from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

default_args = {
    "owner": "fabian.ipsa",
    "start_date": datetime(2024, 1, 1),
}


with DAG(
    dag_id="a10_prod_dag_cims_csv_to_snowflake",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["cims", "azure", "snowflake"],
    description="Loads CIMS.csv from Azure Blob etlia/gfcorporate into CORPCONT.PUBLIC.CIMS.",
) as dag:

    @task(task_id="a01_load_cims_csv_to_snowflake")
    def load_cims_csv_to_snowflake():
        delete_sql = """
            DELETE FROM CORPCONT.PUBLIC.CIMS
            WHERE 1=1;
        """

        copy_sql = """
            COPY INTO CORPCONT.PUBLIC.CIMS
            FROM 'azure://etlia.blob.core.windows.net/gfcorporate'
            STORAGE_INTEGRATION = azure_int_corpcont
            FILE_FORMAT = (
                TYPE = CSV
                FIELD_OPTIONALLY_ENCLOSED_BY = '\"'
                PARSE_HEADER = TRUE
            )
            PATTERN = 'CIMS\\.csv.*'
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FORCE = TRUE;
        """

        hook = SnowflakeHook(
            snowflake_conn_id="Snowflake_Key_Pair_Connection_s_gf_paw"
        )
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            cursor.execute(delete_sql)
            cursor.execute(copy_sql)
            print("Loaded CIMS.csv into CORPCONT.PUBLIC.CIMS.")
        finally:
            cursor.close()

    load_cims_csv_to_snowflake()
