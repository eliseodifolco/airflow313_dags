from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import pendulum

@dag(
    dag_id="a09_prod_dag_audimex_audit_status_dashboard",
    start_date=pendulum.datetime(2024, 1, 1, tz="Europe/Rome"),
    schedule="20 * * * *",  # Every hour at minute 20
    catchup=False,
    tags=["audimex", "snowflake", "dashboard"],
)
def a09_prod_dag_audimex_audit_status_dashboard():

    @task(task_id="refresh_audimex_status_input")
    def refresh_status_input():
        """
        Refreshes IA.AUDIMEX_SOURCE.TBL_AUDMX_AUDIT_STATUS_INPUT.
        """
        hook = SnowflakeHook(snowflake_conn_id="Snowflake_Key_Pair_Connection")
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            print("Calling source refresh procedure...")
            cursor.execute("""
                CALL IA.AUDIMEX_SOURCE.PROC_REFRESH_TBL_AUDMX_AUDIT_STATUS_INPUT()
            """)
            print("Source refresh procedure executed successfully.")
        finally:
            cursor.close()

    @task(task_id="refresh_audit_status_reporting_tables")
    def refresh_reporting_tables():
        """
        Refreshes CORE_FACT_TABLE and TBLDUEDATE in IA.PUBLIC_REPORTS.
        """
        hook = SnowflakeHook(snowflake_conn_id="Snowflake_Key_Pair_Connection")
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            print("Calling reporting model refresh procedure...")
            cursor.execute("""
                CALL IA.PUBLIC_REPORTS.PROC_TBL_PBIREP_AUDIT_STATUS_COREFACTTABLE_TBLDUEDATE_UPDATE()
            """)
            print("Reporting model refresh procedure executed successfully.")
        finally:
            cursor.close()

    t_refresh_input = refresh_status_input()
    t_refresh_reporting = refresh_reporting_tables()

    t_refresh_input >> t_refresh_reporting

dag = a09_prod_dag_audimex_audit_status_dashboard()
