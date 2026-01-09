import json
from airflow.decorators import dag, task 
from airflow.utils.dates import days_ago
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
import snowflake.connector
import pandas as pd
import pendulum
import logging


# NOT NEEDED FOR THE MOMENT BUT USEFUL WHEN ADJUSTMENTS WILL BE MORE NUMEROUS
def _import_script_from_json(script_name):
    with open('/home/adm_difolco_e/airflow/includes/a01_dag_audimex_to_snowflake/sql_scripts.json') as json_file:
        dict = json.load(json_file)
        return dict[script_name]


@dag(schedule=None, start_date=days_ago(1),catchup=False)
def a01_prod_dag_manual_adjustments():

    adjustment_wal_state_log = SnowflakeOperator(task_id='n_wal_state_log_adjustment',
        sql=['''UPDATE ia.audimex_source.wal_state_log
                SET CHANGE_DATE = '2023-05-15'
                WHERE id = 109612'''],
        snowflake_conn_id='Snowflake_Key_Pair_Connection',
        warehouse='IA',
        database='IA',
        role='IA_PIPE_ADMIN',
        schema='AUDIMEX_SOURCE',
        authenticator=None,
        session_parameters=None,
        )    

a01_prod_dag_manual_adjustments()
    
