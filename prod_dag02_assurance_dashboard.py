import json
from airflow.decorators import dag, task 
from airflow.utils.dates import days_ago
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

def _run_swf_script(sql_script):
    cs=cn.cursor()
    cs.execute(sql_script)
    sfqid = cs.sfqid
    return sfqid     

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

def get_df_from_query(sql_script):
    '''from a sql query returns a dictionary of data in tight format to be reused'''
    cs=cn.cursor()
    cs.execute(sql_script)
    df=cs.fetch_pandas_all()
    data_dict = df.to_dict('tight')
    return data_dict

sql_audit_manual_slicer = '''INSERT INTO IA.PUBLIC_REPORTS.PBIREP_ASSURANCE_AUDIT_MANUAL_SLICER(SORT_ORDER_A, CASCATED_AUDIT_MANUAL)
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
ORDER BY sort_order_a'''

sql_audit_chapter_slicer = _yaml_to_sql_script('/home/adm_difolco_e/airflow/includes/a02_dag_assurance_dashboard/audit_chapter_query.yaml', 'sql_audit_chapter_append')

sql_audit_procedure_slicer = _yaml_to_sql_script('/home/adm_difolco_e/airflow/includes/a02_dag_assurance_dashboard/audit_procedure_query.yaml', 'sql_audit_procedure_append')

sql_auditors_consistency = '''SELECT a.A_NUMBER ,b.firstname,b.lastname, count(*) as rowcount 
FROM ia.AUDIMEX_SOURCE.AUDITING a
LEFT JOIN ia.AUDIMEX_SOURCE.AUDITOR b
ON a.LEADING_AUDITOR_ID=b.id
GROUP BY a.A_NUMBER ,b.firstname,b.lastname
ORDER BY rowcount desc'''

@dag(schedule="@daily", start_date=days_ago(1),catchup=False)
def a02_prod_dag_assurance_dashboard():

    core_table_load = SnowflakeOperator(
        task_id='n_update_core_tbl',
        sql=['DELETE FROM IA.PUBLIC_REPORTS.PBIREP_ASSURANCE_CORE_FACT_TABLE WHERE 1=1',
             _yaml_to_sql_script('/home/adm_difolco_e/airflow/includes/a02_dag_assurance_dashboard/core_table_query.yaml', 'sql_core_table_append')],
        snowflake_conn_id='Snowflake_Key_Pair_Connection',
        warehouse='IA',
        database='IA',
        role='IA_PIPE_ADMIN',
        schema='AUDIMEX_SOURCE',
        authenticator=None,
        session_parameters=None,
        )

    audit_manual_slicer_load = SnowflakeOperator(
        task_id='n_update_audit_manual_slicer_tbl',
        sql=['DELETE FROM IA.PUBLIC_REPORTS.PBIREP_ASSURANCE_AUDIT_MANUAL_SLICER WHERE 1=1',
             sql_audit_manual_slicer],
        snowflake_conn_id='Snowflake_Key_Pair_Connection',
        warehouse='IA',
        database='IA',
        role='IA_PIPE_ADMIN',
        schema='AUDIMEX_SOURCE',
        authenticator=None,
        session_parameters=None,
        )

    audit_chapter_slicer_load = SnowflakeOperator(
        task_id='n_update_audit_chapter_slicer_tbl',
        sql=['DELETE FROM IA.PUBLIC_REPORTS.PBIREP_ASSURANCE_AUDIT_CHAPTER_SLICER WHERE 1=1',
             sql_audit_chapter_slicer],
        snowflake_conn_id='Snowflake_Key_Pair_Connection',
        warehouse='IA',
        database='IA',
        role='IA_PIPE_ADMIN',
        schema='AUDIMEX_SOURCE',
        authenticator=None,
        session_parameters=None,
        )

    audit_procedure_slicer_load = SnowflakeOperator(
        task_id='n_update_audit_procedure_slicer_tbl',
        sql=['DELETE FROM IA.PUBLIC_REPORTS.PBIREP_ASSURANCE_AUDIT_PROCEDURE_SLICER WHERE 1=1',
             sql_audit_procedure_slicer],
        snowflake_conn_id='Snowflake_Key_Pair_Connection',
        warehouse='IA',
        database='IA',
        role='IA_PIPE_ADMIN',
        schema='AUDIMEX_SOURCE',
        authenticator=None,
        session_parameters=None,
        )

    @task(task_id="auditor_pandas_analysis")
    def auditor_consistency_check(sql_code):
        input_dict= get_df_from_query(sql_code)
        df = pd.DataFrame.from_dict(input_dict,orient='tight')
        # returns number of responsible auditors per audit, if >1 then there is a data model consistency issue
        nr_auditors = df['ROWCOUNT'].max().item()
        # print(type(nr_auditors))
        return nr_auditors

    auditor_consistency = auditor_consistency_check(sql_auditors_consistency)

a02_prod_dag_assurance_dashboard()