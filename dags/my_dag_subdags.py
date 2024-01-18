import json
from datetime import datetime, timedelta
from airflow.decorators import dag,task
from airflow.operators.subdag import SubDagOperator
from typing import Dict
from subdags.subdag_factory import subdag_factory

@task.python(task_id="extract_subdags",do_xcom_push=False)
def extract() -> Dict[str, str]:
    partner_name = "netflix"
    partner_path = "partners/netflix"
    return {"partner_name": partner_name, "partner_path": partner_path}

default_args = {
    "start_date": datetime(2023, 1, 1),
}
@dag(
    dag_id = "my_dag_subdags",
    schedule="@once",
    default_args=default_args,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
    tags=["DE"],
    max_active_runs=1
    )
def my_dag_subdags():

    process_tasks = SubDagOperator(
        task_id="process_tasks",
        subdag=subdag_factory(
            "my_dag_subdags",
            "process_tasks",
            default_args
            ),
        poke_interval=2
    )

    extract() >> process_tasks

my_dag_subdags()

