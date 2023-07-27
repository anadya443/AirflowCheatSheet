#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Example DAG demonstrating setting up inter-DAG dependencies using ExternalTaskSensor and
ExternalTaskMarker

In this example, child_task1 in example_external_task_marker_child depends on parent_task in
example_external_task_marker_parent. When parent_task is cleared with "Recursive" selected,
the presence of ExternalTaskMarker tells Airflow to clear child_task1 and its
downstream tasks.

ExternalTaskSensor will keep poking for the status of remote ExternalTaskMarker task at a regular
interval till one of the following will happen:
1. ExternalTaskMarker reaches the states mentioned in the allowed_states list
    In this case, ExternalTaskSensor will exit with a success status code
2. ExternalTaskMarker reaches the states mentioned in the failed_states list
    In this case, ExternalTaskSensor will raise an AirflowException and user need to handle this
    with multiple downstream tasks
3. ExternalTaskSensor times out
    In this case, ExternalTaskSensor will raise AirflowSkipException or AirflowSensorTimeout
    exception
"""

import pendulum

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskMarker, ExternalTaskSensor
from airflow.models import DagRun
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

today_date = datetime.now()
def get_most_recent_run_today(dt):
    dag_runs = list(DagRun.find(dag_id="parent_task_anadya"))
    dag_runs.sort(key=lambda x: x.execution_date, reverse=True)
    if dag_runs:
        recent_run = dag_runs[0].execution_date;
        if (recent_run.date() == today_date.date()):
            return recent_run;
        else: 
            return today_date;



last_run_today = get_most_recent_run_today(datetime(2023, 7, 26))

start_date = pendulum.datetime(2021, 1, 1, tz="UTC")


with DAG(
    dag_id="child_anadya",
    start_date=start_date,
    schedule_interval=None,
    catchup=False,
    tags=['anadya'],
) as child_dag:
    # [START howto_operator_external_task_sensor]
    child_task1 = ExternalTaskSensor(
        task_id="child_task_anadya",
        external_dag_id="parent_task_anadya",
        external_task_id="print_date",
        allowed_states=['success'],
        execution_date_fn=get_most_recent_run_today
    )

    child_task2 = BashOperator(
        task_id='print_date',
        depends_on_past=False,
        bash_command=f"echo hello {last_run_today}",
        retries=3,
    )
    # [END howto_operator_external_task_sensor]
    #child_task2 = EmptyOperator(task_id="child_task2")
    child_task2 >> child_task1
