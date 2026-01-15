import json
from airflow.decorators import dag, task
from airflow import AirflowException
from airflow.utils.dates import days_ago
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.xcom_arg import XComArg
import snowflake.connector
import pandas as pd
import numpy
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization

credentials = json.load(open("/home/adm_difolco_e/airflow/includes/credentials.json"))

pwd_encryption = bytes(credentials["Snowflake"]["key_pwd"], 'utf-8')

#reads the p8 formatted key and decrypts it
with open("/home/adm_difolco_e/.ssh/rsa_2048_pkcs8.p8", "rb") as key:
    p_key= serialization.load_pem_private_key(
        key.read(),
        password=pwd_encryption,
        backend=default_backend()
    )

# transform key in bytes returning it in DER format
pkb = p_key.private_bytes(
    encoding=serialization.Encoding.DER,
    format=serialization.PrivateFormat.PKCS8,
    encryption_algorithm=serialization.NoEncryption()
)

cn = snowflake.connector.connect(
    user='IA_PIPE_ADMIN',
    account='cm07628.west-europe.azure',
    private_key=pkb,
    warehouse='IA',
    database='IA'
    )


sql_code_task_history_v = '''SELECT * FROM IA.SWF_ADMIN.TASK_HISTORY_V'''



def get_df_from_query(sql_script,file_name):
    '''from a sql query produces a pkl file named file_name'''
    cs=cn.cursor()
    cs.execute(sql_script)
    df=cs.fetch_pandas_all()
    data_dict = df.to_dict('tight')
    df.to_pickle('/home/adm_difolco_e/airflow/includes/a05_swf_task_control'+'/'+file_name+'.pkl')

task_names=["PBIREP_AUDIT_STATUS_TIMING_CORE_FACT_TABLE_DELETE",
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

removed_tasks = ["PBIREP_AUDIT_FINDING_STATUS_CORE_FACT_TABLE_DELETE",
"PBIREP_AUDIT_FINDING_STATUS_CORE_FACT_TABLE_INSERT",
"PBIREP_AUDIT_FINDING_STATUS_TBLDUEDATE_DELETE",
"PBIREP_AUDIT_FINDING_STATUS_TBLDUEDATE_INSERT",]

@dag(schedule="30 6 * * *", start_date=days_ago(1),catchup=False)
def a05_prod_dag_swf_daily_tasks_control():

    '''eventually I preferred to use a list from the file in pbi, where execution order is checked
    if we change our mind here is the task'''
    # @task(task_id="n_get_swf_task_list")
    # def get_swf_task_list():
    #     sql_code_get_tasks = '''select distinct name from IA.SWF_ADMIN.TASKS order by name'''
    #     cs=cn.cursor()
    #     cs.execute(sql_code_get_tasks)
    #     df=cs.fetch_pandas_all()
    #     task_names = df['NAME'].tolist()
    #     return task_names


    @task(task_id="n_get_today_date")
    def get_today_date():
        '''returns todays date in string as timestamp are not json serializable'''
        import datetime
        '''standardizing to utc for comparison with other functions'''
        today=datetime.datetime.now()
        # today=datetime.datetime.utcnow()
        todaystr = today.strftime('%Y-%m-%d')
        return todaystr

    @task(task_id="n_generate_task_history_json")
    def generate_task_history_json():
        get_df_from_query(sql_code_task_history_v, 'task_history_v')

    @task(task_id="n_get_task_execution_date_from_task_history_v")
    def get_task_execution_date(task_name):
        '''taking the latest execution date for a specific task, date is returned as string as in get_todays_date
        whenever we need to compare hours, we need to take the whole dummy timestamp in string and then re-transform it 
        in the next function. This is because timestamp are not json serializable'''
        df=pd.read_pickle('/home/adm_difolco_e/airflow/includes/a05_swf_task_control/task_history_v.pkl')
        filter_df = df[df['NAME']==task_name]
        '''need to evaluate if normalize to utc makes sense once we have established suitable times for the 
        prod and the control dag run'''
        # pd.to_datetime(filter_df.QUERY_START_TIME).dt.tz_convert('UTC')
        dummy_datetime = pd.to_datetime(filter_df.QUERY_START_TIME).dt.tz_localize(None)
        latest_timestamp = filter_df['QUERY_START_TIME'].max()
        try:
            latest_date = latest_timestamp.strftime('%Y-%m-%d')
        except:
            latest_date = ''
        return [task_name , latest_date]

    @task(task_id="n_date_comparison")
    def date_comparison(iterable, date_str):
        if iterable[1] == date_str:
            return iterable[0] + ' has run on '+ date_str        
        else:
            raise AirflowException('the task '+ iterable[0]+ ' did not run on '+ date_str)
        
    @task(task_id="n_task_list_status", trigger_rule= TriggerRule.ALL_DONE)
    def task_list_status(generator):
        lst = []
        for entry in generator:
            lst.append(entry)
        return lst

          
    # task_names = get_swf_task_list()
    today_date = get_today_date()
    task_history_json = generate_task_history_json()
    task_execution_date= get_task_execution_date.expand(task_name = task_names)
    #task_run_current_date = date_comparison.expand(iterable = task_execution_date)
    task_run_current_date = date_comparison.partial(date_str=today_date).expand(iterable = task_execution_date)
    task_list_status = task_list_status(task_run_current_date)
    
    # task_date_comparison = date_comparison_outcome(today_date,task_execution_date)
    

    today_date >> task_history_json >> task_execution_date >> task_run_current_date >> task_list_status

dag = a05_prod_dag_swf_daily_tasks_control()
