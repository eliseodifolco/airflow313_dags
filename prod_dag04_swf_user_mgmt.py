
import json
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
import snowflake.connector
import pandas as pd
import time
from datetime import datetime
import pendulum
import re
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

def get_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id="Snowflake_Key_Pair_Connection")
    return hook.get_conn()


sql_script_insert_tbl_swf_users = '''INSERT INTO IA.SANDBOX_SOURCE.TBL_SWF_USERS("name", "created_on", "login_name", "display_name", "first_name", "last_name", "email", "mins_to_unlock", "days_to_expiry", "comment", "disabled", "must_change_password", "snowflake_lock", "default_warehouse", "default_namespace", "default_role", "default_secondary_roles", "ext_authn_duo", "ext_authn_uid", "mins_to_bypass_mfa", "owner", "last_success_login", "expires_at_time", "locked_until_time", "has_password", "has_rsa_public_key"
)
SELECT 	"name", "created_on", "login_name", "display_name", "first_name", "last_name", "email", "mins_to_unlock", "days_to_expiry", "comment", "disabled", "must_change_password", "snowflake_lock", "default_warehouse", "default_namespace", "default_role", "default_secondary_roles", "ext_authn_duo", "ext_authn_uid", "mins_to_bypass_mfa", "owner", "last_success_login", "expires_at_time", "locked_until_time", "has_password", "has_rsa_public_key"
FROM TABLE(RESULT_SCAN('{query_id}'))'''

def run_sql(script):
    cn = get_snowflake_conn()
    cs = cn.cursor()
    qry = cs.execute(script)


def run_sql_script_with_id(script):
    '''
    :param script: string made up of sql script
    :return: query id in snowflake
    '''
    cn = get_snowflake_conn()
    cs = cn.cursor()
    qry = cs.execute(script)
    time.sleep(1)
    sfqid = cs.sfqid  # here the query id is picked up from current status of the cursor
    return sfqid

def sql_script_insert_tbl_swf_users_reader():
    sfqid = '{{ti.xcom_pull(key="return_value", task_ids="n_show_users_query")}}'
    sql_code = sql_script_insert_tbl_swf_users.format(query_id=sfqid)
    return sql_code    

''' "0 20 * * 1-5" means monday to friday at 00.20 '''
@dag(schedule="15 1 * * 2-6", start_date=pendulum.datetime(2023, 5, 31, tz="CET"),catchup=False)
def a04_prod_dag_swf_user_mgmt():

    @task(task_id="n_show_users_query")
    def run_show_users_query():
        run_sql('use role accountadmin')
        query_id = run_sql_script_with_id('SHOW USERS')
        return query_id
    
    show_users = run_show_users_query()

    
    @task(task_id="n_generate_current_swf_users_table")
    def generate_current_users_table():
        hook = SnowflakeHook(snowflake_conn_id="Snowflake_Key_Pair_Sys_Connection")
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        # Ensure the correct context (the new SQLExecute operator does NOT set these)
        cursor.execute("USE DATABASE IA;")
        cursor.execute("USE SCHEMA SANDBOX_SOURCE;")
        cursor.execute("USE WAREHOUSE IA;")

        # Step 1: Delete existing rows
        cursor.execute("DELETE FROM IA.SANDBOX_SOURCE.TBL_SWF_USERS WHERE 1=1;")

        # Step 2: Insert new rows using the query ID from SHOW USERS
        sql_insert = sql_script_insert_tbl_swf_users_reader()
        cursor.execute(sql_insert)

        cursor.close()
        
    @task(task_id="n_get_user_deletion_dictionary")
    def get_users_to_delete():
        '''from a SWF view produces a dictionary with the users to be removed from Snowflake'''
        cn = get_snowflake_conn()
        cs=cn.cursor()
        cs.execute("SELECT * FROM IA.AUDIMEX_SOURCE.LST_DELETE_USERS_SWF_V")
        df=cs.fetch_pandas_all()
        data_dict = df.to_dict('tight')
        return data_dict
    
    @task(task_id="n_user_deletion_routine")
    def user_deletion_routine(input_dict):
        '''runs a drop user statement for each user in the input dictionary'''
        df = pd.DataFrame.from_dict(input_dict,orient='tight')
        cn = get_snowflake_conn()
        cs=cn.cursor()
        cs.execute('USE ROLE ACCOUNTADMIN')
        sql_drop_user = 'DROP USER IF EXISTS {username}'
        users_deleted = 0
        for index, row in df.iterrows():
            if type(row['EMAIL']) == str:
                a= re.search('.+(?=@)', row['EMAIL'])
                user_name = "".join(re.findall('\w', a.group(0)))
                cs.execute(sql_drop_user.format(username=user_name))
                users_deleted+=1
        return users_deleted
    
    user_delete_dictionary = get_users_to_delete()
    drop_users = user_deletion_routine(user_delete_dictionary)

    @task(task_id="n_get_user_creation_dictionary")
    def get_users_to_create():
        cn = get_snowflake_conn()
        cs=cn.cursor()
        cs.execute("SELECT * FROM IA.AUDIMEX_SOURCE.LST_CREATE_USERS_SWF_V")
        df=cs.fetch_pandas_all()
        data_dict = df.to_dict('tight')
        return data_dict

    @task(task_id="n_user_creation_routine")
    def user_creation_routine(input_dict):
        '''runs a drop create user statement for each user in the input dictionary'''
        df = pd.DataFrame.from_dict(input_dict,orient='tight')
        cn = get_snowflake_conn()
        cs=cn.cursor()
        cs.execute('USE ROLE ACCOUNTADMIN')
        sql_create_user = """Create or replace user {username}
            LOGIN_NAME=  '{email}'
            DISPLAY_NAME = '{username}'
            EMAIL = '{email}'
            DEFAULT_ROLE = '{role}'
            DEFAULT_SECONDARY_ROLES =('ALL')
            COMMENT = 'AUDIMEX_AUTO' """
        sql_grant_role = "grant role report_reader to user {}"
        users_created = 0
        for index, row in df.iterrows():
            if type(row['EMAIL']) == str:
                a= re.search('.+(?=@)', row['EMAIL'])
                user_name = "".join(re.findall('\w', a.group(0))).upper()        
                # print(user_name)
                # print(sql_create_user.format(username = user_name, email = row['EMAIL'], role = 'REPORT_READER'))
                cs.execute(sql_create_user.format(username = user_name, email = row['EMAIL'], role = 'REPORT_READER'))
                cs.execute(sql_grant_role.format(user_name))
                users_created+=1
        return users_created
    
    generate_current_users_table = generate_current_users_table()
    user_create_dictionary = get_users_to_create()
    create_users = user_creation_routine(user_create_dictionary)

    show_users >> generate_current_users_table >> user_delete_dictionary >> drop_users >> user_create_dictionary >> create_users

dag = a04_prod_dag_swf_user_mgmt()
