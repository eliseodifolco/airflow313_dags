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
    
    @task(task_id="run_swf_data_model_proc")
    def run_stored_procedure():
        """
        Calls the stored procedure that refreshes the audit status model in Snowflake.
        """
        hook = SnowflakeHook(snowflake_conn_id="Snowflake_Key_Pair_Connection")
        conn = hook.get_conn()
        cursor = conn.cursor()

        try:
            print("Calling Snowflake stored procedure...")
            cursor.execute("""
                CALL IA.PUBLIC_REPORTS.PROC_TBL_PBIREP_AUDIT_STATUS_COREFACTTABLE_TBLDUEDATE_UPDATE()
            """)
            print("Procedure executed successfully.")
        finally:
            cursor.close()

    run_stored_procedure()

dag = a09_prod_dag_audimex_audit_status_dashboard()
