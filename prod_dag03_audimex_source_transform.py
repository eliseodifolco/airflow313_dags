import re
import yaml
import pendulum
import pandas as pd

from airflow.decorators import dag, task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


# --------------------------------------------------------------------------
# Helpers
# --------------------------------------------------------------------------

def get_snowflake_cursor():
    hook = SnowflakeHook(snowflake_conn_id="Snowflake_Key_Pair_Connection")
    conn = hook.get_conn()
    cs = conn.cursor()
    cs.execute("USE DATABASE IA")
    cs.execute("USE SCHEMA AUDIMEX_SOURCE")
    cs.execute("USE WAREHOUSE IA")
    cs.execute("USE ROLE IA_PIPE_ADMIN")
    return cs


def load_sql_script(sql_file_name: str) -> str:
    with open(
        f"/home/adm_difolco_e/air_disk/airflow/includes/a03_dag_audimex_source_transform/{sql_file_name}",
        "r",
    ) as f:
        return re.sub(r"\t|\n", " ", f.read())


def yaml_to_sql_script(yaml_file_path: str, dict_key: str) -> str:
    with open(yaml_file_path) as f:
        content = yaml.load(f, Loader=yaml.FullLoader)
    return content[dict_key]


# --------------------------------------------------------------------------
# SQL constants
# --------------------------------------------------------------------------

sql_audmx_division_tree_append = """
INSERT INTO IA.AUDIMEX_SOURCE.TBL_AUDMX_DIVISION_TREE (cc_sort_order, division, business_unit)
WITH
div (cc_sort_order, division) AS (
    SELECT
        ML.cc_sort_order,
        CASE
            WHEN TRIM(LEFT(ml.CC_SORT_ORDER,4)) = 1 THEN 'GFPS'
            WHEN TRIM(LEFT(ml.CC_SORT_ORDER,4)) = 2 THEN 'GFCS'
            WHEN TRIM(LEFT(ml.CC_SORT_ORDER,4)) = 3 THEN 'GFMS'
            WHEN TRIM(LEFT(ml.CC_SORT_ORDER,4)) = 4 THEN 'CORP'
            WHEN TRIM(LEFT(ml.CC_SORT_ORDER,4)) = 5 THEN 'NonStandard'
            WHEN TRIM(LEFT(ml.CC_SORT_ORDER,4)) = 6 THEN 'GFUP'
            WHEN TRIM(LEFT(ml.CC_SORT_ORDER,4)) = 11 THEN 'GFPS[IT]'
            WHEN TRIM(LEFT(ml.CC_SORT_ORDER,4)) = 12 THEN 'GFCS[IT]'
            WHEN TRIM(LEFT(ml.CC_SORT_ORDER,4)) = 13 THEN 'GFMS[IT]'
            WHEN TRIM(LEFT(ml.CC_SORT_ORDER,4)) = 14 THEN 'CORP[IT]'
            WHEN TRIM(LEFT(ml.CC_SORT_ORDER,4)) = 15 THEN 'NonStandard[IT]'
            ELSE 'no_division'
        END AS division
    FROM IA.AUDIMEX_SOURCE.TBL_ENTITIES_MASTER_LIST ML
),
bu (business_unit, cc_sort_order) AS (
    SELECT ml.ENTITY_NAME, ml.CC_SORT_ORDER
    FROM IA.AUDIMEX_SOURCE.TBL_ENTITIES_MASTER_LIST ml
    WHERE ml.AU_TYPE = 'Business Unit'
)
SELECT
    div.cc_sort_order,
    div.division,
    bu.business_unit
FROM bu
RIGHT JOIN div
    ON bu.cc_sort_order = LEFT(div.cc_sort_order,9)
"""

sql_audmx_auditing_to_au_append = """
INSERT INTO IA.AUDIMEX_SOURCE.TBL_AUDMX_AUDITING_TO_DIVISION (AUDITING_ID, CC_SORT_ORDER, DIVISION)
SELECT
    AUD.ID AS auditing_id,
    ML.cc_sort_order,
    CASE
        WHEN TRIM(LEFT(ml.CC_SORT_ORDER,4)) = 1 THEN 'GFPS'
        WHEN TRIM(LEFT(ml.CC_SORT_ORDER,4)) = 2 THEN 'GFCS'
        WHEN TRIM(LEFT(ml.CC_SORT_ORDER,4)) = 3 THEN 'GFMS'
        WHEN TRIM(LEFT(ml.CC_SORT_ORDER,4)) = 4 THEN 'CORP'
        WHEN TRIM(LEFT(ml.CC_SORT_ORDER,4)) = 5 THEN 'NonStandard'
        WHEN TRIM(LEFT(ml.CC_SORT_ORDER,4)) = 6 THEN 'GFUP'
        WHEN TRIM(LEFT(ml.CC_SORT_ORDER,4)) = 11 THEN 'GFPS[IT]'
        WHEN TRIM(LEFT(ml.CC_SORT_ORDER,4)) = 12 THEN 'GFCS[IT]'
        WHEN TRIM(LEFT(ml.CC_SORT_ORDER,4)) = 13 THEN 'GFMS[IT]'
        WHEN TRIM(LEFT(ml.CC_SORT_ORDER,4)) = 14 THEN 'CORP[IT]'
        WHEN TRIM(LEFT(ml.CC_SORT_ORDER,4)) = 15 THEN 'NonStandard[IT]'
        ELSE 'no_division'
    END AS division
FROM IA.AUDIMEX_SOURCE.AUDITING AUD
LEFT JOIN IA.AUDIMEX_SOURCE.CONTAINS_PU CPU
    ON AUD.ID = CPU.AUDITING_ID
LEFT JOIN IA.AUDIMEX_SOURCE.AUDIT_UNIVERSE AU
    ON CPU.PLANNING_UNIT_ID = AU.ID
LEFT JOIN IA.AUDIMEX_SOURCE.TBL_ENTITIES_MASTER_LIST ML
    ON AU.CC_SORT_ORDER = ML.CC_SORT_ORDER
GROUP BY
    AUD.ID,
    ml.cc_sort_order,
    division
"""

sql_audit_manual_tree_consistency = """
SELECT sort_order, COUNT(*) AS rowcount
FROM IA.AUDIMEX_SOURCE.TBL_AUDMX_AUDIT_MANUAL_TREE
WHERE sort_order IS NOT NULL
GROUP BY sort_order
"""

# --------------------------------------------------------------------------
# DAG
# --------------------------------------------------------------------------

@dag(
    dag_id="a03_prod_dag_audimex_source_transform",
    schedule="10 1 * * *",
    start_date=pendulum.datetime(2024, 1, 1, tz="UTC"),
    catchup=False,
    tags=["audimex", "snowflake", "transform"],
)
def a03_prod_dag_audimex_source_transform():

    @task
    def update_audit_manual_tree():
        cs = get_snowflake_cursor()
        cs.execute("DELETE FROM IA.AUDIMEX_SOURCE.TBL_AUDMX_AUDIT_MANUAL_TREE")
        cs.execute(load_sql_script("sql_audit_manual_tree_append.txt"))
        cs.close()

    @task
    def audit_manual_tree_consistency_check():
        cs = get_snowflake_cursor()
        cs.execute(sql_audit_manual_tree_consistency)
        df = cs.fetch_pandas_all()
        cs.close()

        if df["ROWCOUNT"].max() < 2:
            print("Audit manual tree model is consistent")
        else:
            raise Exception("Audit manual tree model inconsistency detected")

    @task
    def update_tbl_tree_entities():
        cs = get_snowflake_cursor()
        cs.execute("DELETE FROM IA.AUDIMEX_SOURCE.TBL_TREE_ENTITIES")
        cs.execute(
            yaml_to_sql_script(
                "/home/adm_difolco_e/air_disk/airflow/includes/a03_dag_audimex_source_transform/sql_tbl_tree_entities_append.yaml",
                "sql_tbl_tree_entities_append",
            )
        )
        cs.close()

    @task
    def update_tbl_entities_master_list():
        cs = get_snowflake_cursor()
        cs.execute("DELETE FROM IA.AUDIMEX_SOURCE.TBL_ENTITIES_MASTER_LIST")
        cs.execute(
            yaml_to_sql_script(
                "/home/adm_difolco_e/air_disk/airflow/includes/a03_dag_audimex_source_transform/sql_tbl_entities_master_list_append.yaml",
                "sql_tbl_entities_master_list_append",
            )
        )
        cs.close()

    @task
    def update_tbl_auditing_to_au():
        cs = get_snowflake_cursor()
        cs.execute("DELETE FROM IA.AUDIMEX_SOURCE.TBL_AUDMX_AUDITING_TO_DIVISION")
        cs.execute(sql_audmx_auditing_to_au_append)
        cs.close()

    @task
    def update_tbl_division_tree():
        cs = get_snowflake_cursor()
        cs.execute("DELETE FROM IA.AUDIMEX_SOURCE.TBL_AUDMX_DIVISION_TREE")
        cs.execute(sql_audmx_division_tree_append)
        cs.close()

    @task
    def audit_division_tree_consistency_check():
        cs = get_snowflake_cursor()
        cs.execute(
            "SELECT ENTITY_NAME, CC_SORT_ORDER "
            "FROM IA.AUDIMEX_SOURCE.TBL_ENTITIES_MASTER_LIST "
            "WHERE AU_TYPE = 'Business Unit'"
        )
        df = cs.fetch_pandas_all()
        cs.close()

        if df.CC_SORT_ORDER.str.len().min() != 9 or df.CC_SORT_ORDER.str.len().max() != 9:
            raise Exception("Invalid CC_SORT_ORDER length detected")

        if not df[df.duplicated("CC_SORT_ORDER")].empty:
            raise Exception("Duplicated CC_SORT_ORDER detected")

        print("Division tree model is consistent")

    @task
    def update_tbl_audit_manual():
        cs = get_snowflake_cursor()
        cs.execute("DELETE FROM IA.AUDIMEX_SOURCE.TBL_AUDMX_AUDIT_MANUAL")
        cs.execute(
            yaml_to_sql_script(
                "/home/adm_difolco_e/air_disk/airflow/includes/a03_dag_audimex_source_transform/sql_tbl_audmx_audit_manual_append.yaml",
                "sql_tbl_audmx_audit_manual_append",
            )
        )
        cs.close()

    @task
    def update_tbl_tree_flat():
        cs = get_snowflake_cursor()
        cs.execute("DELETE FROM IA.AUDIMEX_SOURCE.TBL_TREE_FLAT")
        cs.execute(
            yaml_to_sql_script(
                "/home/adm_difolco_e/air_disk/airflow/includes/a03_dag_audimex_source_transform/sql_tbl_tree_flat_append.yaml",
                "sql_tbl_tree_flat_append",
            )
        )
        cs.close()

    trigger_a02 = TriggerDagRunOperator(
        task_id="n_trigger_dag_a02",
        trigger_dag_id="a02_prod_dag_assurance_dashboard",
    )

    trigger_a10 = TriggerDagRunOperator(
        task_id="n_trigger_dag_a10",
        trigger_dag_id="a10_prod_call_root_tasks_SWF",
    )

    # Wiring
    t_manual = update_audit_manual_tree()
    t_manual_check = audit_manual_tree_consistency_check()
    t_entities_tree = update_tbl_tree_entities()
    t_entities_master = update_tbl_entities_master_list()
    t_manual_tbl = update_tbl_audit_manual()
    t_flat_tbl = update_tbl_tree_flat()
    t_div_check = audit_division_tree_consistency_check()
    t_aud_to_au = update_tbl_auditing_to_au()
    t_div_tree = update_tbl_division_tree()

    # Logical execution chain
    (
        t_manual
        >> t_manual_check
        >> [t_entities_tree, t_entities_master, t_manual_tbl]
        >> t_flat_tbl
        >> t_div_check
        >> t_aud_to_au
        >> t_div_tree
        >> trigger_a02
        >> trigger_a10
    )


dag = a03_prod_dag_audimex_source_transform()
