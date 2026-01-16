import json
from airflow.decorators import dag, task 
from airflow.utils.dates import days_ago
#from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.dummy import DummyOperator
from airflow.operators.empty import EmptyOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
import snowflake.connector
import pandas as pd
import pendulum
import logging
import yaml
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


sql_script_TBL_AUDMX_AUDIT_STATUS_TIMING_INPUT_consistency = '''SELECT CITY, POSTAL_CODE, COUNTRY, COUNTRY_CODE, CC_SORT_ORDER, AUDIT_NUMBER, AUDIT_TITLE, AUDIT_STATUS, WS_SORT_ORDER, WS_STATUS_RANK, 
LEAD_AUDITOR, AUDITOR, METATIME, FROM_STATUS, TO_STATUS, CHANGE_DATE,
count(*) AS rowcount
FROM IA.AUDIMEX_SOURCE.TBL_AUDMX_AUDIT_STATUS_TIMING_INPUT
GROUP BY CITY, POSTAL_CODE, COUNTRY, COUNTRY_CODE, CC_SORT_ORDER, AUDIT_NUMBER, AUDIT_TITLE, AUDIT_STATUS, WS_SORT_ORDER, WS_STATUS_RANK, 
LEAD_AUDITOR, AUDITOR, METATIME, FROM_STATUS, TO_STATUS, CHANGE_DATE
ORDER BY rowcount, WS_SORT_ORDER DESC'''

sql_script_TBL_ENTITIES_MASTER_LIST_consistency = ''' SELECT CC_SORT_ORDER, AU_TYPE, count(*) AS rowcount
FROM IA.AUDIMEX_SOURCE.TBL_ENTITIES_MASTER_LIST
GROUP BY CC_SORT_ORDER, AU_TYPE
HAVING rowcount>1'''



def get_df_from_query(sql_script,file_name):
    '''from a sql query produces a pkl file named file_name'''
    cs=cn.cursor()
    cs.execute(sql_script)
    df=cs.fetch_pandas_all()
    data_dict = df.to_dict('tight')
    df.to_pickle('/home/adm_difolco_e/airflow/includes/a05_swf_task_control'+'/'+file_name+'.pkl')

def _yaml_to_sql_script(yaml_file_path,dict_key):
	'''returns a dictionary with the key name specified in the yaml file which has the following format
	key: >
	  sql code
	a key and a text argument with no newlines is returned'''
	with open(yaml_file_path) as file:
		# The FullLoader parameter handles the conversion from YAML
		# scalar values to Python the dictionary format
		content = yaml.load(file, Loader=yaml.FullLoader)
		return content[dict_key]


@dag(schedule="15 2 * * *", start_date=days_ago(1),catchup=False)
def a07_prod_dag_audit_timing_dashboard():

    TBL_AUDMX_AUDIT_STATUS_TIMING_INPUT_pipeline = SnowflakeOperator(task_id='n_load_TBL_AUDMX_AUDIT_STATUS_TIMING_INPUT',
        sql=['delete from IA.AUDIMEX_SOURCE.TBL_AUDMX_AUDIT_STATUS_TIMING_INPUT where 1=1',
		    _yaml_to_sql_script('/home/adm_difolco_e/airflow/includes/a07_audit_timing_dashboard/sql_script_1.yaml','sql_update_TBL_AUDMX_AUDIT_STATUS_TIMING_INPUT')],
        snowflake_conn_id='Snowflake_Key_Pair_Connection',
        warehouse='IA',
        database='IA',
        role='IA_PIPE_ADMIN',
        schema='AUDIMEX_SOURCE',
        authenticator=None,
        session_parameters=None,
        autocommit = True,
        trigger_rule='all_done'
        )
    
    ''' I only need to run task IA.PUBLIC_REPORTS.PBIREP_AUDIT_STATUS_TIMING_CORE_FACT_TABLE_DELETE as this is a root task that will trigger downstream ones'''
    TBL_AUDMX_AUDIT_STATUS_TIMING_INPUT_tasks = SnowflakeOperator(task_id='n_launch_TBL_AUDMX_AUDIT_STATUS_TIMING_INPUT_tasks',
        sql = ['EXECUTE TASK IA.PUBLIC_REPORTS.PBIREP_AUDIT_STATUS_TIMING_CORE_FACT_TABLE_DELETE'],
        snowflake_conn_id='Snowflake_Key_Pair_Connection',
        warehouse='IA',
        database='IA',
        role='IA_PIPE_ADMIN',
        schema='PUBLIC_REPORTS',
        authenticator=None,
        session_parameters=None,
        autocommit = True,
        trigger_rule='all_done'
        )    
    
    @task(task_id="n_TBL_ENTITIES_MASTER_LIST_consistency")
    def TBL_ENTITIES_MASTER_LIST_consistency(sql_script):
        '''I expect CC_SORT_ORDER to be primary key, this check should be sufficient for enrich1'''
        cs=cn.cursor()
        cs.execute(sql_script)
        df=cs.fetch_pandas_all()
        if df.empty:
            print('TBL_ENTITIES_MASTER_LIST is consistent')
        else:
            nr_rows = df['ROWCOUNT'].max().item()
            if nr_rows<2:
                print('TBL_ENTITIES_MASTER_LIST is consistent')        
            elif nr_rows>1:
                raise Exception('there are multiple names for same CC_SORT_ORDER, this would case several inconsistencies')
            
    
    @task(task_id="n_TBL_AUDMX_AUDIT_STATUS_TIMING_INPUT_consistency", trigger_rule='all_done')
    def TBL_AUDMX_AUDIT_STATUS_TIMING_INPUT_consistency(sql_script):
        cs=cn.cursor()
        cs.execute(sql_script)
        df=cs.fetch_pandas_all()
        nr_rows = df['ROWCOUNT'].max().item()
        if df.empty:
            print('TBL_AUDMX_AUDIT_STATUS_TIMING_INPUT is consistent')
        else:
            nr_rows = df['ROWCOUNT'].max().item()             
            if nr_rows>1:
                raise Exception('for each audit same status is repeated')
            
        
    

    TBL_ENTITIES_MASTER_LIST_consistency = TBL_ENTITIES_MASTER_LIST_consistency(sql_script_TBL_ENTITIES_MASTER_LIST_consistency)
    TBL_AUDMX_AUDIT_STATUS_TIMING_INPUT_consistency = TBL_AUDMX_AUDIT_STATUS_TIMING_INPUT_consistency(sql_script_TBL_AUDMX_AUDIT_STATUS_TIMING_INPUT_consistency)

    
    TBL_AUDMX_AUDIT_STATUS_TIMING_INPUT_pipeline >> TBL_AUDMX_AUDIT_STATUS_TIMING_INPUT_tasks >> \
    TBL_ENTITIES_MASTER_LIST_consistency >> TBL_AUDMX_AUDIT_STATUS_TIMING_INPUT_consistency

a07_prod_dag_audit_timing_dashboard()


