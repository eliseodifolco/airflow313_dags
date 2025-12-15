from airflow import DAG, XComArg
from airflow.decorators import dag, task
import snowflake.connector
import os
import time
from datetime import datetime
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.hazmat.primitives.asymmetric import dsa
from cryptography.hazmat.primitives import serialization


# adding or removing the proxy based on the network setup 

# proxy = "http://165.225.240.44:80/"

# os.environ['http_proxy'] = proxy 
# os.environ['HTTP_PROXY'] = proxy
# os.environ['https_proxy'] = proxy
# os.environ['HTTPS_PROXY'] = proxy

# pwd_encryption = b''

pwd_encryption= bytes("",'utf-8')

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

@dag(schedule="30 1 * * *", start_date=datetime(2024, 4, 5), catchup=False)
def test_swf_key_pair_auth():

# ctx = snowflake.connector.connect(
#     user='IA_PIPE_ADMIN',
#     account='cm07628.west-europe.azure',
#     private_key=pkb,
#     warehouse='COMPUTE_WH',
#     database='IA'
#     )

    @task(task_id = "swf_loader")
    def run_test_query():
        import snowflake.connector
        cn = snowflake.connector.connect(user='IA_PIPE_ADMIN',
            account='cm07628.west-europe.azure',
            private_key=pkb,
            warehouse='COMPUTE_WH',
            database='IA'
            )
        cs = cn.cursor()
        cs.execute("use role IA_PIPE_ADMIN")
        cs.execute("use schema IA.AUDIMEX_SOURCE")

    swf_decorator_test=run_test_query()      

test_swf_key_pair_auth()