import pendulum
import pickle

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.models import DagRun
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta, timezone

dag_id="child_dag"
task_id="child_dag_task"
external_dag_id="parent_dag"
external_task_id="parent_dag_task"

def get_most_recent_run_today(dag_id):
    today_date = datetime.now(timezone.utc)
    dag_runs = list(DagRun.find(dag_id=dag_id))
    dag_runs.sort(key=lambda x: x.start_date, reverse=True)
    if dag_runs:
        recent_run_start_date = dag_runs[0].start_date
        recent_run_execution_date = dag_runs[0].execution_date
        if (recent_run_start_date.date() == today_date.date()):
            return recent_run_execution_date;
        else: 
            return today_date;
    else:
        return today_date;


def get_most_recent_dag():
    today_date = datetime.now(timezone.utc)
    dag_runs = list(DagRun.find(dag_id=external_dag_id))
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    return dag_runs[0]

def get_most_recent_external_dag_today(dt):
    return get_most_recent_run_today(external_dag_id);

last_run_today = get_most_recent_external_dag_today(None)
recent_dag = get_most_recent_dag()

with DAG(
    dag_id=dag_id,
    start_date=datetime(2023, 7, 19,0,0,0),
    schedule_interval=None,
    catchup=False,
    tags=["poc_dag_dependency"],
    is_paused_upon_creation=True
) as child_dag:

    child_task1 = ExternalTaskSensor(
        task_id=task_id,
        external_dag_id=external_dag_id,
        external_task_id=external_task_id,
        allowed_states=["success"],
        execution_date_fn=get_most_recent_external_dag_today
    )

    child_task2 = BashOperator(
        task_id="print_date",
        depends_on_past=False,
        bash_command=f"echo hello { recent_dag.start_date} {recent_dag.execution_date}",
        retries=3,
    )
    child_task2 >> child_task1
