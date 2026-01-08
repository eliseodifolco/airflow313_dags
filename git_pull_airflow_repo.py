from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from datetime import datetime

'''this is the case where I still need to set saved credentials for github'''

@dag(schedule=None, start_date=datetime(2025, 1, 1), catchup=False)
def git_pull_dag_repo():
    BashOperator(
        task_id="git_pull",
        bash_command="git -C /home/adm_difolco_e/air_disk/airflow/dags pull",
    )

dag = git_pull_dag_repo()
