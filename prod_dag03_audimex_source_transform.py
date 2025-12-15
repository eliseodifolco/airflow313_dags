import json
from airflow.decorators import dag, task 
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.contrib.operators.snowflake_operator import SnowflakeOperator
import snowflake.connector
import pandas as pd
import re
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


def load_sql_script(sql_file_name):
    with open('/home/adm_difolco_e/airflow/includes/a03_dag_audimex_source_transform/' + sql_file_name, "r") as txtfile:
        txt = txtfile.read()
        qry = re.sub('\t|\n', ' ', txt)
        return qry
    
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


sql_audmx_division_tree_append = '''insert into IA.AUDIMEX_SOURCE.TBL_AUDMX_DIVISION_TREE(cc_sort_order,division,business_unit) 
                                    WITH 
                                    div(cc_sort_order, division) AS (SELECT 
                                    ML.cc_sort_order,
                                    case 
                                        when trim(LEFT(ml.CC_SORT_ORDER,4)) =1 then 'GFPS'
                                        when trim(LEFT(ml.CC_SORT_ORDER,4)) =2 then 'GFCS'
                                        when trim(LEFT(ml.CC_SORT_ORDER,4)) =3 then 'GFMS'
                                        when trim(LEFT(ml.CC_SORT_ORDER,4)) =4 then 'CORP'
                                        when trim(LEFT(ml.CC_SORT_ORDER,4)) =5 then 'NonStandard'
                                        when trim(LEFT(ml.CC_SORT_ORDER,4)) =6 then 'GFUP'
                                        when trim(LEFT(ml.CC_SORT_ORDER,4)) =11 then 'GFPS[IT]'
                                        when trim(LEFT(ml.CC_SORT_ORDER,4)) =12 then 'GFCS[IT]'
                                        when trim(LEFT(ml.CC_SORT_ORDER,4)) =13 then 'GFMS[IT]'
                                        when trim(LEFT(ml.CC_SORT_ORDER,4)) =14 then 'CORP[IT]'
                                        when trim(LEFT(ml.CC_SORT_ORDER,4)) =15 then 'NonStandard[IT]'
                                    ELSE 'no_division'
                                    END AS division
                                    FROM IA.AUDIMEX_SOURCE.TBL_ENTITIES_MASTER_LIST ML)
                                    ,
                                    bu(business_unit, cc_sort_order) AS 
                                            (SELECT ml.ENTITY_NAME, ml.CC_SORT_ORDER FROM IA.AUDIMEX_SOURCE.TBL_ENTITIES_MASTER_LIST ml
                                            WHERE ml.AU_TYPE='Business Unit'
                                            )
                                    SELECT 
                                    div.cc_sort_order,
                                    div.division,
                                    bu.business_unit
                                    FROM bu
                                    right JOIN div
                                    ON bu.cc_sort_order=LEFT(div.cc_sort_order,9)'''

sql_audmx_auditing_to_au_append='''
insert into IA.AUDIMEX_SOURCE.TBL_AUDMX_AUDITING_TO_DIVISION(AUDITING_ID, CC_SORT_ORDER,DIVISION )
SELECT
AUD.ID AS auditing_id,
ML.cc_sort_order,
case 
	when trim(LEFT(ml.CC_SORT_ORDER,4)) =1 then 'GFPS'
	when trim(LEFT(ml.CC_SORT_ORDER,4)) =2 then 'GFCS'
	when trim(LEFT(ml.CC_SORT_ORDER,4)) =3 then 'GFMS'
	when trim(LEFT(ml.CC_SORT_ORDER,4)) =4 then 'CORP'
	when trim(LEFT(ml.CC_SORT_ORDER,4)) =5 then 'NonStandard'
	when trim(LEFT(ml.CC_SORT_ORDER,4)) =6 then 'GFUP'
	when trim(LEFT(ml.CC_SORT_ORDER,4)) =11 then 'GFPS[IT]'
	when trim(LEFT(ml.CC_SORT_ORDER,4)) =12 then 'GFCS[IT]'
	when trim(LEFT(ml.CC_SORT_ORDER,4)) =13 then 'GFMS[IT]'
	when trim(LEFT(ml.CC_SORT_ORDER,4)) =14 then 'CORP[IT]'
	when trim(LEFT(ml.CC_SORT_ORDER,4)) =15 then 'NonStandard[IT]'
ELSE 'no_division'
END AS division
FROM IA.AUDIMEX_SOURCE.AUDITING AUD
LEFT JOIN IA.AUDIMEX_SOURCE.CONTAINS_PU CPU
ON AUD.ID=CPU.AUDITING_ID
LEFT JOIN IA.AUDIMEX_SOURCE.AUDIT_UNIVERSE AU
ON CPU.PLANNING_UNIT_ID=AU.ID
LEFT JOIN IA.AUDIMEX_SOURCE.TBL_ENTITIES_MASTER_LIST ML
ON AU.CC_SORT_ORDER=ML.CC_SORT_ORDER
GROUP BY AUD.ID,
ml.cc_sort_order,
division'''

sql_audit_manual_tree_append_consistency ='''SELECT sort_order, count(*) as rowcount 
from IA.AUDIMEX_SOURCE.TBL_AUDMX_AUDIT_MANUAL_TREE
WHERE SORT_ORDER IS NOT null
GROUP BY sort_order'''

@dag(schedule="10 1 * * *", start_date=days_ago(1),catchup=False)
def a03_prod_dag_audimex_source_transform():

    
    run_query_audit_manual_tree = SnowflakeOperator(
        task_id='n_update_audit_manual_tree',
        sql=['delete from IA.AUDIMEX_SOURCE.TBL_AUDMX_AUDIT_MANUAL_TREE where 1=1', load_sql_script('sql_audit_manual_tree_append.txt')],
        snowflake_conn_id='Snowflake_Key_Pair_Connection',
        warehouse='IA',
        database='IA',
        role='IA_PIPE_ADMIN',
        schema='AUDIMEX_SOURCE',
        authenticator=None,
        session_parameters=None,
        )
    
    @task(task_id="n_audit_manual_tree_data_model_consistency_check")
    def audit_manual_tree_data_model_consistency_check(sql_code):
        cs=cn.cursor()
        cs.execute(sql_code)
        df=cs.fetch_pandas_all()
        nr_rows = df['ROWCOUNT'].max().item()
        if nr_rows<2:
            print('model is consistent')
        else:
            raise Exception('number of guidelines per audit is duplicated')
    
    run_query_tbl_tree_entities = SnowflakeOperator(
        task_id='n_update_tbl_tree_entities',
        sql=['delete from IA.AUDIMEX_SOURCE.TBL_TREE_ENTITIES where 1=1', 
             _yaml_to_sql_script('/home/adm_difolco_e/airflow/includes/a03_dag_audimex_source_transform/sql_tbl_tree_entities_append.yaml',
                                 'sql_tbl_tree_entities_append')],
        snowflake_conn_id='Snowflake_Key_Pair_Connection',
        warehouse='IA',
        database='IA',
        role='IA_PIPE_ADMIN',
        schema='AUDIMEX_SOURCE',
        authenticator=None,
        session_parameters=None,
        )
    
    run_query_tbl_entities_master_list = SnowflakeOperator(
        task_id='n_update_tbl_entities_master_list',
        sql=['delete from IA.AUDIMEX_SOURCE.TBL_ENTITIES_MASTER_LIST where 1=1',
             _yaml_to_sql_script('/home/adm_difolco_e/airflow/includes/a03_dag_audimex_source_transform/sql_tbl_entities_master_list_append.yaml',
                                 'sql_tbl_entities_master_list_append')],
        snowflake_conn_id='Snowflake_Key_Pair_Connection',
        warehouse='IA',
        database='IA',
        role='IA_PIPE_ADMIN',
        schema='AUDIMEX_SOURCE',
        authenticator=None,
        session_parameters=None,
        )

    run_query_tbl_auditing_to_au = SnowflakeOperator(
        task_id='n_update_tbl_audmx_auditing_to_au',
        sql=['delete from IA.AUDIMEX_SOURCE.TBL_AUDMX_AUDITING_TO_DIVISION where 1=1',
             sql_audmx_auditing_to_au_append],
        snowflake_conn_id='Snowflake_Key_Pair_Connection',
        warehouse='IA',
        database='IA',
        role='IA_PIPE_ADMIN',
        schema='AUDIMEX_SOURCE',
        authenticator=None,
        session_parameters=None,
        )

    run_query_tbl_division_tree = SnowflakeOperator(
        task_id='n_update_tbl_audmx_division_tree',
        sql=['delete from IA.AUDIMEX_SOURCE.TBL_AUDMX_DIVISION_TREE where 1=1',
             sql_audmx_division_tree_append],
        snowflake_conn_id='Snowflake_Key_Pair_Connection',
        warehouse='IA',
        database='IA',
        role='IA_PIPE_ADMIN',
        schema='AUDIMEX_SOURCE',
        authenticator=None,
        session_parameters=None,
        )

    @task(task_id="n_audmx_division_tree_consistency_check")
    def audit_division_tree_consistency_check(sql_code):
        cs=cn.cursor()
        cs.execute(sql_code)
        df=cs.fetch_pandas_all()
        # print(df)
        nr_rows_max = df.CC_SORT_ORDER.str.len().max() 
        nr_rows_min = df.CC_SORT_ORDER.str.len().min()
        print(nr_rows_max)
        print(nr_rows_min)
        if nr_rows_max==9 and nr_rows_min==9:
            print('model is consistent')
        else:
            raise Exception('there is a business unit with a sort_code not = to 9 digits') 
        if df[df.duplicated('CC_SORT_ORDER')].empty:
            '''this method returns a dataframe and we check if it is empty'''
            print('no duplicated cc_sort_order')
        else:
            raise Exception('duplicated sort order in the model')
    
    audit_division_tree_consistency=audit_division_tree_consistency_check("SELECT ENTITY_NAME, CC_SORT_ORDER FROM IA.AUDIMEX_SOURCE.TBL_ENTITIES_MASTER_LIST WHERE AU_TYPE='Business Unit'")
    
    run_query_tbl_audmx_audit_manual = SnowflakeOperator(
        task_id='n_update_tbl_audmx_audit_manual',
        sql=['delete from IA.AUDIMEX_SOURCE.TBL_AUDMX_AUDIT_MANUAL where 1=1', 
             _yaml_to_sql_script('/home/adm_difolco_e/airflow/includes/a03_dag_audimex_source_transform/sql_tbl_audmx_audit_manual_append.yaml',
                                 'sql_tbl_audmx_audit_manual_append')],
        snowflake_conn_id='Snowflake_Key_Pair_Connection',
        warehouse='IA',
        database='IA',
        role='IA_PIPE_ADMIN',
        schema='AUDIMEX_SOURCE',
        authenticator=None,
        session_parameters=None,
        )
    
    run_query_tbl_tree_flat = SnowflakeOperator(
        task_id='n_update_tbl_tree_flat',
        sql=['delete from IA.AUDIMEX_SOURCE.TBL_TREE_FLAT where 1=1', 
             _yaml_to_sql_script('/home/adm_difolco_e/airflow/includes/a03_dag_audimex_source_transform/sql_tbl_tree_flat_append.yaml',
                                 'sql_tbl_tree_flat_append')],
        snowflake_conn_id='Snowflake_Key_Pair_Connection',
        warehouse='IA',
        database='IA',
        role='IA_PIPE_ADMIN',
        schema='AUDIMEX_SOURCE',
        authenticator=None,
        session_parameters=None,
        )

    trigger_dependent_dag_a02 = TriggerDagRunOperator(
        task_id='n_trigger_dag_a02',
        trigger_dag_id='a02_prod_dag_assurance_dashboard'
    )

    trigger_dependent_dag_a10 = TriggerDagRunOperator(
        task_id='n_trigger_dag_a10',
        trigger_dag_id='a10_prod_call_root_tasks_SWF'
    )

    audit_manual_tree_consistency = audit_manual_tree_data_model_consistency_check(sql_audit_manual_tree_append_consistency)
    
    run_query_audit_manual_tree >> audit_manual_tree_consistency >> trigger_dependent_dag_a02 >> trigger_dependent_dag_a10
    # run_query_tbl_tree_entities
    audit_division_tree_consistency >> run_query_tbl_auditing_to_au >> run_query_tbl_division_tree

dag = a03_prod_dag_audimex_source_transform()