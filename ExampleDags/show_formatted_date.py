import pendulum
import pickle

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.models import DagRun
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta, timezone

dag_id="show_formatted_date"
task_id="task1"
task_id_next = "task2"


with DAG(
    dag_id=dag_id,
    start_date=datetime(2023, 7, 19,0,0,0),
    schedule_interval=None,
    catchup=False,
    tags=["poc_dag_dependency"],
    is_paused_upon_creation=True
) as dag:

    date_interval_end_str_utc = "{{ data_interval_end.strftime('%Y-%m-%d %H:%M:%S') }}"
    

    child_task1 = BashOperator(
        task_id=task_id,
        depends_on_past=False,
        bash_command=f"echo data_interval_end { date_interval_end_str_utc }.",
        retries=3,
    )

    child_task1
