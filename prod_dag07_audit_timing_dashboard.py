import yaml
import pendulum
import pandas as pd

from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

def get_snowflake_cursor():
    hook = SnowflakeHook(snowflake_conn_id="Snowflake_Key_Pair_Connection")
    conn = hook.get_conn()
    cs = conn.cursor()
    cs.execute("USE DATABASE IA")
    cs.execute("USE SCHEMA AUDIMEX_SOURCE")
    cs.execute("USE WAREHOUSE IA")
    cs.execute("USE ROLE IA_PIPE_ADMIN")
    return cs


def yaml_to_sql_script(yaml_file_path: str, dict_key: str) -> str:
    with open(yaml_file_path) as f:
        content = yaml.load(f, Loader=yaml.FullLoader)
    return content[dict_key]

# --------------------------------------------------------------------------
# Constants
# --------------------------------------------------------------------------

SQL_ENTITIES_CHECK = """
SELECT CC_SORT_ORDER, AU_TYPE, COUNT(*) AS rowcount
FROM IA.AUDIMEX_SOURCE.TBL_ENTITIES_MASTER_LIST
GROUP BY CC_SORT_ORDER, AU_TYPE
HAVING COUNT(*) > 1
"""

SQL_TIMING_CHECK = """
SELECT CITY, POSTAL_CODE, COUNTRY, COUNTRY_CODE, CC_SORT_ORDER, AUDIT_NUMBER,
       AUDIT_TITLE, AUDIT_STATUS, WS_SORT_ORDER, WS_STATUS_RANK, 
       LEAD_AUDITOR, AUDITOR, METATIME, FROM_STATUS, TO_STATUS, CHANGE_DATE,
       COUNT(*) AS rowcount
FROM IA.AUDIMEX_SOURCE.TBL_AUDMX_AUDIT_STATUS_TIMING_INPUT
GROUP BY CITY, POSTAL_CODE, COUNTRY, COUNTRY_CODE, CC_SORT_ORDER, AUDIT_NUMBER,
         AUDIT_TITLE, AUDIT_STATUS, WS_SORT_ORDER, WS_STATUS_RANK, 
         LEAD_AUDITOR, AUDITOR, METATIME, FROM_STATUS, TO_STATUS, CHANGE_DATE
ORDER BY rowcount DESC
"""

YAML_INSERT_PATH = "/home/adm_difolco_e/air_disk/airflow/includes/a07_audit_timing_dashboard/sql_script_1.yaml"
YAML_KEY = "sql_update_TBL_AUDMX_AUDIT_STATUS_TIMING_INPUT"

# --------------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------------

@dag(
    dag_id="a07_prod_dag_audit_timing_dashboard",
    schedule="20 * * * *",  # Every hour at 20 minutes past
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["audit", "timing", "snowflake"],
)
def a07_prod_dag_audit_timing_dashboard():

    @task
    def load_timing_input_table():
        cs = get_snowflake_cursor()
        cs.execute("DELETE FROM IA.AUDIMEX_SOURCE.TBL_AUDMX_AUDIT_STATUS_TIMING_INPUT WHERE 1=1")
        cs.execute(yaml_to_sql_script(YAML_INSERT_PATH, YAML_KEY))
        cs.close()

    @task
    def trigger_downstream_snowflake_task():
        cs = get_snowflake_cursor()
        cs.execute("EXECUTE TASK IA.PUBLIC_REPORTS.PBIREP_AUDIT_STATUS_TIMING_CORE_FACT_TABLE_DELETE")
        cs.close()

    @task
    def check_entities_consistency():
        cs = get_snowflake_cursor()
        cs.execute(SQL_ENTITIES_CHECK)
        df = cs.fetch_pandas_all()
        cs.close()

        if df.empty:
            print("TBL_ENTITIES_MASTER_LIST is consistent")
        else:
            raise Exception("Inconsistencies found in TBL_ENTITIES_MASTER_LIST")

    @task
    def check_timing_input_consistency():
        cs = get_snowflake_cursor()
        cs.execute(SQL_TIMING_CHECK)
        df = cs.fetch_pandas_all()
        cs.close()

        if df.empty:
            print("TBL_AUDMX_AUDIT_STATUS_TIMING_INPUT is consistent")
        else:
            max_count = df["ROWCOUNT"].max()
            if max_count > 1:
                raise Exception("Duplicate audit status transitions found")

    # Wiring
    t1 = load_timing_input_table()
    t2 = trigger_downstream_snowflake_task()
    t3 = check_entities_consistency()
    t4 = check_timing_input_consistency()

    t1 >> t2 >> t3 >> t4


dag = a07_prod_dag_audit_timing_dashboard()
