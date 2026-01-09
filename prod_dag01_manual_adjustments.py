import pendulum
from airflow.decorators import dag, task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


@dag(
    dag_id="a01_prod_dag_manual_adjustments",
    schedule=None,
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["audimex", "manual", "adjustment"],
)
def a01_prod_dag_manual_adjustments():

    @task
    def apply_adjustment():
        """
        Performs a manual update on WAL_STATE_LOG table.
        """
        hook = SnowflakeHook(snowflake_conn_id="Snowflake_Key_Pair_Connection")
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Set context
        cursor.execute("USE WAREHOUSE IA")
        cursor.execute("USE ROLE IA_PIPE_ADMIN")
        cursor.execute("USE DATABASE IA")
        cursor.execute("USE SCHEMA AUDIMEX_SOURCE")

        # Execute adjustment
        cursor.execute("""
            UPDATE WAL_STATE_LOG
            SET CHANGE_DATE = '2023-05-15'
            WHERE ID = 109612
        """)

        cursor.close()
        print("Manual adjustment applied.")

    apply_adjustment()


dag = a01_prod_dag_manual_adjustments()
