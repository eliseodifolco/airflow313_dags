import json
import pendulum
import pandas as pd

from airflow.decorators import dag, task
from airflow import AirflowException
from airflow.utils.trigger_rule import TriggerRule
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

task_names = [
    "PBIREP_AUDIT_STATUS_TIMING_CORE_FACT_TABLE_DELETE",
    "PBIREP_AUDIT_STATUS_TIMING_CORE_FACT_TABLE_INSERT",
    "PBIREP_AUDIT_STATUS_TIMING_CORE_FACT_TABLE_TRANSFORM",
    "PBIREP_AUDIT_STATUS_TIMING_STATUS_AGGREGATION_TABLE_DELETE",
    "PBIREP_AUDIT_STATUS_TIMING_STATUS_AGGREGATION_TABLE_INSERT",
    "PBIREP_AUDIT_STATUS_TIMING_STATUS_MOVING_AVERAGE_TABLE_DELETE",
    "PBIREP_AUDIT_STATUS_TIMING_STATUS_MOVING_AVERAGE_TABLE_INSERT",
    "PBIREP_FINDING_BY_AUDIT_MANUAL_CATEGORY_TASK_DELETE",
    "PBIREP_FINDING_BY_AUDIT_MANUAL_CATEGORY_TASK_INSERT",
    "SECURITY_TBL_MAP_AUD_TREE_DELETE",
    "SECURITY_TBL_MAP_AUD_TREE_INSERT",
    "SECURITY_TBL_MAP_AUD_TREE_INSERT_CORP_CONTR"
]

removed_tasks = [
    "PBIREP_AUDIT_FINDING_STATUS_CORE_FACT_TABLE_DELETE",
    "PBIREP_AUDIT_FINDING_STATUS_CORE_FACT_TABLE_INSERT",
    "PBIREP_AUDIT_FINDING_STATUS_TBLDUEDATE_DELETE",
    "PBIREP_AUDIT_FINDING_STATUS_TBLDUEDATE_INSERT",
]

sql_code_task_history_v = "SELECT * FROM IA.SWF_ADMIN.TASK_HISTORY_V"

def get_snowflake_cursor():
    """Use SnowflakeHook to return a properly scoped Snowflake cursor."""
    hook = SnowflakeHook(snowflake_conn_id="Snowflake_Key_Pair_Connection")
    conn = hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("USE ROLE IA_PIPE_ADMIN")
    cursor.execute("USE WAREHOUSE IA")
    cursor.execute("USE DATABASE IA")
    cursor.execute("USE SCHEMA SWF_ADMIN")
    return cursor

def get_df_from_query(sql_script: str, file_name: str):
    """From a SQL query, produce a pickle file with the result DataFrame."""
    cs = get_snowflake_cursor()
    cs.execute(sql_script)
    df = cs.fetch_pandas_all()
    df.to_pickle(f"/home/adm_difolco_e/airflow/includes/a05_swf_task_control/{file_name}.pkl")
    cs.close()

@dag(
    schedule="10 * * * *",  # Daily at 06:30
    start_date=pendulum.datetime(2023, 5, 31, tz="CET"),
    catchup=False,
    tags=["swf", "snowflake", "task_control"]
)
def a05_prod_dag_swf_daily_tasks_control():

    @task(task_id="n_get_today_date")
    def get_today_date():
        from datetime import datetime
        return datetime.now().strftime('%Y-%m-%d')

    @task(task_id="n_generate_task_history_json")
    def generate_task_history_json():
        get_df_from_query(sql_code_task_history_v, "task_history_v")

    @task(task_id="n_get_task_execution_date_from_task_history_v")
    def get_task_execution_date(task_name: str):
        df = pd.read_pickle("/home/adm_difolco_e/airflow/includes/a05_swf_task_control/task_history_v.pkl")
        filter_df = df[df["NAME"] == task_name]
        try:
            latest_timestamp = pd.to_datetime(filter_df["QUERY_START_TIME"]).dt.tz_localize(None).max()
            latest_date = latest_timestamp.strftime('%Y-%m-%d')
        except Exception:
            latest_date = ""
        return [task_name, latest_date]

    @task(task_id="n_date_comparison")
    def date_comparison(iterable, date_str):
        task_name, run_date = iterable
        if run_date == date_str:
            return f"{task_name} has run on {date_str}"
        else:
            raise AirflowException(f"Task {task_name} did NOT run on {date_str}")

    @task(task_id="n_task_list_status", trigger_rule=TriggerRule.ALL_DONE)
    def task_list_status(generator_output):
        return list(generator_output)

    # DAG flow
    today = get_today_date()
    hist = generate_task_history_json()
    exec_dates = get_task_execution_date.expand(task_name=task_names)
    comparisons = date_comparison.partial(date_str=today).expand(iterable=exec_dates)
    summary = task_list_status(comparisons)

    today >> hist >> exec_dates >> comparisons >> summary

dag = a05_prod_dag_swf_daily_tasks_control()
