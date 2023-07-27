from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.models import DagRun

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag_id='parent_dag'
task_id='parent_dag_task'
with DAG(
    dag_id=dag_id,
    default_args=default_args,
    description='parent dag poc dag dependency',
    start_date=datetime(2023, 7, 19,0,0,0),
    catchup=False,
    schedule_interval=None,
    tags=['poc_dag_dependency'],
    is_paused_upon_creation=True
) as dag:
 
    t1 = BashOperator(
        task_id=task_id,
        bash_command='date x',
    )

  
