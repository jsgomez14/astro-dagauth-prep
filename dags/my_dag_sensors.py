from datetime import datetime, timedelta
from airflow.decorators import dag,task
from task_groups.process_tasks import process_tasks
from airflow.sensors.date_time import DateTimeSensor
from airflow.exceptions import AirflowTaskTimeout, AirflowSensorTimeout
from typing import Dict
import time

partners = {
    "partner_snowflake" : { 
            "name" : "snowflake",
            "path" : "parnets/snowflake",
            "priority" : 1
        },
    "partner_netflix" : {
            "name" : "netflix",
            "path" : "parnets/netflix",
            "priority" : 2
        },
    "partner_amazon" : {
            "name" : "amazon",
            "path" : "parnets/amazon",
            "priority" : 3
        },
}

def _success_callback(context):
    print(context)
    print("Success")

def _failure_callback(context):
    if context['exception']:
        if isinstance(context['exception'],AirflowTaskTimeout):
            print("Task timeout")
        if isinstance(context['exception'],AirflowSensorTimeout):
            print("Sensor timeout")
    print(context)
    print("Failure")

def _retry_callback(context):
    if context['ti'].try_number() > 2:
        print("Retrying for the third time or more...")
    print(context)
    print("Retry")

default_args = {
    "start_date": datetime(2023, 1, 1),
}

@dag(
    schedule="@daily",
    default_args=default_args,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10), 
    tags=["DE"],
    max_active_runs=1
    )
def my_dag_sensors():

    delay = DateTimeSensor(
        task_id="delay",
        target_time="{{ execution_date.add(hours=9) }}",
        poke_interval=60 * 60,
        mode='reschedule', # poke: waits until condition is met. reschedule: reschedules the task.
        timeout=60 * 60 * 10,
        # execution_timeout=, # Task duration
        # soft_fail=True, # If sensor meets the timeout, soft fail skips sensor and won't fail.
        # exponential_backoff=True # Poke with incremental intervals.
        # on_success_callback=_success_callback, # DAG Level Callbacks
        # on_failure_callback=_failure_callback
    )
    for partner, details in partners.items():
        @task.python(task_id=f"extract_{partner}",do_xcom_push=False,
            pool='partner_pool',
            pool_slots=1,
            priority_weight = details['priority'],
            on_success_callback=_success_callback,
            on_failure_callback=_failure_callback,
            on_retry_callback=_retry_callback,
            retries=3,
            retry_delay=timedelta(minutes=5),
            # retry_exponential_backoff=True # Retries with incremental intervals. For API calls for example.
            # max_retry_delay=timedelta(minutes=15), # Maximum time for exponential backoff.
            ) # Task level callbacks.
        def extract(partner_name, partner_path) -> Dict[str, str]:
            time.sleep(5)
            return {"partner_name": partner_name, "partner_path": partner_path}
        extract = extract(details['name'], details['path'])
        process_tasks(extract)
        delay >> extract
    # Downstream, upstream, absolute priorities
my_dag_sensors()

