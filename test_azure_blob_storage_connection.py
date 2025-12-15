from airflow import DAG
from airflow.decorators import task
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from datetime import datetime
import os

with DAG(
    dag_id="test_task5_upload_to_azure_blob",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "azure", "wasb"],
) as dag:

    @task
    def upload_test_file():
        # File to upload (make sure it exists!)
        local_file_path = "/home/adm_difolco_e/airflow/includes/a01_dag_audimex_to_snowflake/test_upload.parquet"
        blob_name = "test_upload.parquet"
        container_name = "etlloadtoswf"

        if not os.path.exists(local_file_path):
            # Create a small dummy file for testing
            with open(local_file_path, "wb") as f:
                f.write(b"This is a test parquet placeholder.\n")

        # Connect using WasbHook
        hook = WasbHook(wasb_conn_id="azure_blob_etlia")
        with open(local_file_path, "rb") as f:
            hook.upload(
                container_name=container_name,
                blob_name=blob_name,
                data=f,
                overwrite=True
            )

        print(f"✅ Successfully uploaded {blob_name} to Azure Blob container: {container_name}")

    upload_test_file()
