from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import os
import json
from airflow.exceptions import AirflowException
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import json
import os
import re

default_args = {
    "owner": "eliseo.difolco",
    "start_date": datetime(2024, 1, 1),
}

tbl_name_list = ['activity', 'guideline', 'auditing','wal_state','issue', 'suggestion','auditor','auditee','wal_state_log','wal_user','audit_universe','reserve_sel_issue_00']  # Easily extendable
schema_name = 'public'


with DAG(
    dag_id="prod_dag01_audimex_tbls_to_snowflake",
    default_args=default_args,
    schedule=None,
    catchup=False,
    tags=["audimex", "postgres", "snowflake"],
    description="Extracts metadata and records from Postgres Audimex tables, prepares data for Snowflake load.",
) as dag:

    @task
    def a01_snowflake_field_metadata(tbl_name: str):
        """
        Retrieves column names and data types from Snowflake information_schema
        for the destination table. Identifies boolean and date fields.
        """
        from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

        database = "IA"
        schema = "AUDIMEX_SOURCE"

        hook = SnowflakeHook(snowflake_conn_id="Snowflake_Key_Pair_Connection")
        conn = hook.get_conn()
        cursor = conn.cursor()

        sql = f"""
            SELECT column_name, data_type
            FROM {database}.information_schema.columns
            WHERE table_schema = '{schema}'
            AND table_name = '{tbl_name.upper()}'
            ORDER BY ordinal_position
        """
        cursor.execute(sql)
        results = cursor.fetchall()

        columns = []
        types = []
        boolean_fields = []
        date_fields = []

        for col_name, data_type in results:
            col_lc = col_name.lower()
            dt = data_type.lower()

            columns.append(col_lc)
            types.append(dt)

            if dt == "boolean":
                boolean_fields.append(col_lc)
            elif dt in ("date", "datetime", "timestamp", "timestamp_ntz",
                        "timestamp_ltz", "timestamp_tz"):
                date_fields.append(col_lc)

        cursor.close()

        return {
            "columns": columns,
            "types": types,
            "boolean_fields": boolean_fields,
            "date_fields": date_fields
        }

    
    @task
    def a02_bytearray_spotter(field_meta: dict, tbl_name: str):
        """
        Generates SELECT clause using Snowflake metadata, inserting mock values
        for missing or BYTEA columns from Postgres.
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook

        hook = PostgresHook(postgres_conn_id="postgres_audimex")
        conn = hook.get_conn()
        cursor = conn.cursor()

        # Get actual columns in Postgres
        cursor.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public'
            AND table_name = '{tbl_name}'
        """)
        pg_columns = {row[0]: row[1] for row in cursor.fetchall()}
        cursor.close()

        pg_column_names = set(pg_columns.keys())  # lowercase by default

        select_clause = []

        for col in field_meta["columns"]:  # Snowflake columns, likely uppercase
            col_lc = col.lower()
            if col_lc not in pg_column_names:
                # Column exists in Snowflake but not in Postgres
                print(f"Mocking missing column: {col_lc}")
                select_clause.append(f"'' AS {col_lc}")
            elif pg_columns[col_lc] == 'bytea':
                print(f"Skipping BYTEA field: {col_lc}")
                select_clause.append(f"'' AS {col_lc}")
            else:
                select_clause.append(f'"{col_lc}"')

        sql = f'SELECT {", ".join(select_clause)} FROM "public"."{tbl_name}"'
        print(f"\nGenerated SQL:\n{sql}\n")
        return sql


    @task
    def a03_query_table(sql_query: str, tbl_name: str, sf_meta: dict):
        """
        Executes the SQL query from Postgres and writes result to JSON.
        Converts boolean fields to true/false and normalizes date fields.
        Uses Snowflake metadata (lowercase column names).
        """
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        import json
        import os

        out_dir = "/home/adm_difolco_e/air_disk/airflow/includes/a01_dag_audimex_to_snowflake"
        os.makedirs(out_dir, exist_ok=True)
        out_file = f"{out_dir}/{tbl_name}.json"

        hook = PostgresHook(postgres_conn_id="postgres_audimex")
        conn = hook.get_conn()
        cursor = conn.cursor()
        cursor.execute(sql_query)

        bool_fields = sf_meta.get("boolean_fields", [])
        date_fields = sf_meta.get("date_fields", [])
        # Identify numeric fields based on Snowflake types
        numeric_fields = [col.lower() for col, dtype in zip(sf_meta["columns"], sf_meta["types"]) if dtype in ("number", "int", "integer", "decimal", "numeric")]

        column_names = [desc[0].lower() for desc in cursor.description]

        records = []
        row_count = 0

        while True:
            rows = cursor.fetchmany(1000)
            if not rows:
                break

            for row in rows:
                row_dict = dict(zip(column_names, row))

                for field in bool_fields:
                    if field in row_dict:
                        val = row_dict[field]
                        row_dict[field] = bool(int(val)) if val is not None else False

                for field in date_fields:
                    if field in row_dict and row_dict[field] is not None:
                        row_dict[field] = str(row_dict[field])[:10]



                # Replace None with 0 in numeric fields
                for field in numeric_fields:
                    if field in row_dict and row_dict[field] is None:
                        row_dict[field] = 0

                records.append(row_dict)
                row_count += 1

        cursor.close()

        with open(out_file, "w", encoding="utf-8") as f:
            json.dump(records, f, ensure_ascii=False, indent=2)

        print(f"Wrote {row_count} rows to {out_file}")
        return out_file

    

    @task(task_id='a04_json_to_parquet_with_transform')
    def json_to_parquet_w_transform(json_file_path: str):
        """
        Loads JSON into pandas, transforms content, and writes Parquet
        using pandas-inferred schema (booleans preserved).
        """
        import pandas as pd
        import pyarrow as pa
        import pyarrow.parquet as pq
        import json
        import os
        import re

        print(f"Loading JSON: {json_file_path}")
        with open(json_file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        df = pd.DataFrame(data)

        # Step 1: Remove Mandarin characters (if present)
        cjk_re = re.compile(r"[\u3400-\u4DBF\u4E00-\u9FFF\uF900-\uFAFF]")

        for col in ["finding_title", "action_description", "change_comment"]:
            if col in df.columns:
                df[col] = df[col].astype(str).apply(lambda x: cjk_re.sub("", x))


        print("Mandarin removed (if any)")

        # Step 2: Date field normalization (keep format YYYY-MM-DD)
        for col in df.columns:
            if col.endswith("_date") or col.endswith("_when") or col == "suggested_next_au":
                df[col] = (
                    df[col]
                    .astype(str)
                    .str.replace("/", "-", regex=False)     # replaces slashes with dashes
                    .str[:10]                                # ensures string is max 10 characters
                    .str.replace("--", "", regex=False)      # removes any stray double-dash
                )


        # Step 3: Print detected dtypes
        print("Pandas dtypes (inferred):")
        for col in df.columns:
            print(f"{col}: {df[col].dtype}")

        # Step 4: Write to Parquet using inferred schema
        parquet_path = json_file_path.replace(".json", ".parquet")
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_table(table, parquet_path)
        print(f"Parquet written: {parquet_path}")

        # check if any column contains date format yyyy/mm/dd - result at the last lines of the log for each table in task 4

        import re
        date_pattern_slash = re.compile(r"^\d{4}/\d{2}/\d{2}$")

        offending_fields = set()
        for col in df.columns:
            if df[col].dtype == object:
                if df[col].dropna().apply(lambda x: bool(date_pattern_slash.match(str(x)))).any():
                    offending_fields.add(col)

        if offending_fields:
            print("\nFields still containing slashed dates (YYYY/MM/DD):")
            for field in sorted(offending_fields):
                print(f"  - {field}")
        else:
            print("\nNo fields with YYYY/MM/DD format found.")



        # Regex pattern for dates in format YYYY/MM/DD
        print("Scanning for values in format YYYY/MM/DD:")

        offenders = []

        for col in df.columns:
            if df[col].dtype == object:
                try:
                    mask = df[col].astype(str).str.contains(r"\d{4}/\d{2}/\d{2}", regex=True, na=False)
                    if mask.any():
                        print(f"Found YYYY/MM/DD pattern in column: {col}")
                        offenders.append(col)
                        print(df.loc[mask, col].head(3))  # Print sample offenders
                except Exception as e:
                    print(f"Error scanning column {col}: {e}")



        # Step 5: Delete JSON
        try:
            os.remove(json_file_path)
            print(f"Deleted JSON: {json_file_path}")
        except Exception as e:
            print(f"Could not delete JSON: {e}")

    

    @task(task_id="a05_upload_to_azure_blob")
    def upload_parquet_to_azure(tbl_name: str):
        """
        Uploads a Parquet file to Azure Blob Storage using WasbHook.
        """
        local_path = f"/home/adm_difolco_e/air_disk/airflow/includes/a01_dag_audimex_to_snowflake/{tbl_name}.parquet"
        blob_name = f"{tbl_name}.parquet"
        container_name = "etlloadtoswf"

        if not os.path.exists(local_path):
            raise FileNotFoundError(f"Parquet file not found: {local_path}")

        hook = WasbHook(wasb_conn_id="azure_blob_etlia")
        with open(local_path, "rb") as f:
            hook.upload(
                container_name=container_name,
                blob_name=blob_name,
                data=f,
                overwrite=True
            )

        print(f"Uploaded {blob_name} to Azure Blob container '{container_name}'")


    @task(task_id="a06_load_to_snowflake")
    def load_parquet_to_snowflake(tbl_name: str):
        """
        Loads Parquet data from Azure Blob into Snowflake using COPY INTO with the
        file format IA.AUDIMEX_SOURCE.PARQUET_FORMAT.
        """

        delete_sql = f"""
            DELETE FROM IA.AUDIMEX_SOURCE.{tbl_name}
            WHERE 1=1;
            """

        copy_sql = f"""
            COPY INTO IA.AUDIMEX_SOURCE.{tbl_name}
            FROM 'azure://etlia.blob.core.windows.net/etlloadtoswf'
            STORAGE_INTEGRATION = azure_int
            FILE_FORMAT = (FORMAT_NAME = IA.AUDIMEX_SOURCE.PARQUET_FORMAT)
            PATTERN = '{tbl_name}\\.parquet.*'
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
            FORCE = TRUE;
            """

        hook = SnowflakeHook(snowflake_conn_id="Snowflake_Key_Pair_Connection")
        conn = hook.get_conn()
        cursor = conn.cursor()
        
        try:
            print(f"Deleting existing data from table {tbl_name} ...")
            cursor.execute(delete_sql)
            print(f"Executing COPY INTO for table {tbl_name} ...")
            cursor.execute(copy_sql)
            print(f"Loaded {tbl_name} into Snowflake.")
        finally:
            cursor.close()


    @task(task_id="a07_cleanup_blob_parquet")
    def delete_parquet_blob(tbl_name: str):
        """
        Deletes the uploaded Parquet file from Azure Blob Storage after loading into Snowflake.
        """
        blob_name = f"{tbl_name}.parquet"
        container_name = "etlloadtoswf"

        hook = WasbHook(wasb_conn_id="azure_blob_etlia")
        blob_service_client = hook.get_conn()
        container_client = blob_service_client.get_container_client(container=container_name)
        blob_client = container_client.get_blob_client(blob=blob_name)

        if blob_client.exists():
            blob_client.delete_blob()
            print(f"Deleted blob: {blob_name}")
        else:
            print(f"Blob not found, nothing to delete: {blob_name}")



    
    # DAG wiring
    # Example test list
    

    for tbl_name in tbl_name_list:
        meta = a01_snowflake_field_metadata.override(task_id=f"a01_{tbl_name}_meta")(tbl_name)
        sql = a02_bytearray_spotter.override(task_id=f"a02_{tbl_name}_sql")(meta, tbl_name)
        json_path = a03_query_table.override(task_id=f"a03_{tbl_name}_query")(sql, tbl_name, meta)
        parquet = json_to_parquet_w_transform.override(task_id=f"a04_{tbl_name}_to_parquet")(json_path)
        upload = upload_parquet_to_azure.override(task_id=f"a05_{tbl_name}_upload")(tbl_name)
        load_swf = load_parquet_to_snowflake.override(task_id=f"a06_{tbl_name}_copy_into")(tbl_name)
        cleanup = delete_parquet_blob.override(task_id=f"a07_{tbl_name}_cleanup")(tbl_name)
        meta >> sql >> json_path >> parquet >> upload >> load_swf >> cleanup

        # json_path = a03_query_table.override(task_id=f"a03_{tbl_name}_query")(sql, tbl_name, meta)
        # parquet = json_to_parquet_w_transform.override(task_id=f"a04_{tbl_name}_to_parquet")(json_path, meta)
        # upload = upload_parquet_to_azure.override(task_id=f"a05_{tbl_name}_upload")(tbl_name)
        # load_swf = load_parquet_to_snowflake.override(task_id=f"a06_{tbl_name}_copy_into")(tbl_name)
        # cleanup = delete_parquet_blob.override(task_id=f"a07_{tbl_name}_cleanup")(tbl_name)

        # meta >> sql >> json_path >> parquet >> upload >> load_swf >> cleanup
