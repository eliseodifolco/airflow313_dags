import yaml
import pendulum
import pandas as pd

from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


# --------------------------------------------------------------------------
# Helpers (aligned with dag03)
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
    """
    YAML file format:
      some_key: >
        SQL CODE ...
    Returns the SQL string for dict_key.
    """
    with open(yaml_file_path) as f:
        content = yaml.load(f, Loader=yaml.FullLoader)
    return content[dict_key]


# --------------------------------------------------------------------------
# Paths (match the style used in dag03)
# --------------------------------------------------------------------------

INCLUDES_DIR = "/home/adm_difolco_e/air_disk/airflow/includes/a02_dag_assurance_dashboard"

YAML_CORE_TABLE = f"{INCLUDES_DIR}/core_table_query.yaml"
YAML_AUDIT_CHAPTER = f"{INCLUDES_DIR}/audit_chapter_query.yaml"
YAML_AUDIT_PROCEDURE = f"{INCLUDES_DIR}/audit_procedure_query.yaml"


# --------------------------------------------------------------------------
# SQL constants (same logic as your dag02)
# --------------------------------------------------------------------------

SQL_AUDIT_MANUAL_SLICER_APPEND = """
INSERT INTO IA.PUBLIC_REPORTS.PBIREP_ASSURANCE_AUDIT_MANUAL_SLICER (SORT_ORDER_A, CASCATED_AUDIT_MANUAL)
SELECT
    CASE
        WHEN sort_order_a = ' 243' THEN ' 043'
        WHEN sort_order_a = ' 251' THEN ' 051'
        WHEN sort_order_a = ' 252' THEN ' 252'
        WHEN sort_order_a = ' 452' THEN ' 252'
        WHEN sort_order_a = ' 272' THEN ' 072'
        ELSE sort_order_a
    END,
    CASCATED_AUDIT_MANUAL
FROM IA.AUDIMEX_SOURCE.TBL_AUDMX_AUDIT_MANUAL_TREE
GROUP BY SORT_ORDER_A, CASCATED_AUDIT_MANUAL
ORDER BY sort_order_a
"""

SQL_AUDITORS_CONSISTENCY = """
SELECT a.A_NUMBER, b.firstname, b.lastname, COUNT(*) AS rowcount
FROM IA.AUDIMEX_SOURCE.AUDITING a
LEFT JOIN IA.AUDIMEX_SOURCE.AUDITOR b
    ON a.LEADING_AUDITOR_ID = b.id
GROUP BY a.A_NUMBER, b.firstname, b.lastname
ORDER BY rowcount DESC
"""


# --------------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------------

@dag(
    dag_id="a02_prod_dag_assurance_dashboard",
    schedule="@daily",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["assurance", "snowflake", "reports"],
)
def a02_prod_dag_assurance_dashboard():

    @task
    def update_core_tbl():
        cs = get_snowflake_cursor()
        cs.execute("DELETE FROM IA.PUBLIC_REPORTS.PBIREP_ASSURANCE_CORE_FACT_TABLE WHERE 1=1")
        cs.execute(yaml_to_sql_script(YAML_CORE_TABLE, "sql_core_table_append"))
        cs.close()

    @task
    def update_audit_manual_slicer_tbl():
        cs = get_snowflake_cursor()
        cs.execute("DELETE FROM IA.PUBLIC_REPORTS.PBIREP_ASSURANCE_AUDIT_MANUAL_SLICER WHERE 1=1")
        cs.execute(SQL_AUDIT_MANUAL_SLICER_APPEND)
        cs.close()

    @task
    def update_audit_chapter_slicer_tbl():
        cs = get_snowflake_cursor()
        cs.execute("DELETE FROM IA.PUBLIC_REPORTS.PBIREP_ASSURANCE_AUDIT_CHAPTER_SLICER WHERE 1=1")
        cs.execute(yaml_to_sql_script(YAML_AUDIT_CHAPTER, "sql_audit_chapter_append"))
        cs.close()

    @task
    def update_audit_procedure_slicer_tbl():
        cs = get_snowflake_cursor()
        cs.execute("DELETE FROM IA.PUBLIC_REPORTS.PBIREP_ASSURANCE_AUDIT_PROCEDURE_SLICER WHERE 1=1")
        cs.execute(yaml_to_sql_script(YAML_AUDIT_PROCEDURE, "sql_audit_procedure_append"))
        cs.close()

    @task
    def auditor_pandas_analysis():
        """
        Returns the max number of responsible auditors per audit.
        If > 1 then there's a potential data model consistency issue (same as your current intent).
        """
        cs = get_snowflake_cursor()
        cs.execute(SQL_AUDITORS_CONSISTENCY)
        df = cs.fetch_pandas_all()
        cs.close()

        nr_auditors = int(df["ROWCOUNT"].max())
        return nr_auditors

    # Wiring (same semantics as before: loads + analysis)
    t_core = update_core_tbl()
    t_manual = update_audit_manual_slicer_tbl()
    t_chapter = update_audit_chapter_slicer_tbl()
    t_proc = update_audit_procedure_slicer_tbl()
    t_aud_check = auditor_pandas_analysis()

    [t_core, t_manual, t_chapter, t_proc] >> t_aud_check


dag = a02_prod_dag_assurance_dashboard()
