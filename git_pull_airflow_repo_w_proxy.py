from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="dag_git_pull_with_proxy",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["utility", "git", "proxy"],
) as dag:

    git_pull = BashOperator(
        task_id="pull_dags_repo",
        bash_command="""
            cd /home/adm_difolco_e/air_disk/airflow/dags \
            && GIT_CONFIG_SYSTEM=/dev/null git -c safe.directory=/home/adm_difolco_e/air_disk/airflow/dags pull
        """,
        env={
            "http_proxy": "http://165.225.240.44:80/",
            "https_proxy": "http://165.225.240.44:80/",
        },
    )
