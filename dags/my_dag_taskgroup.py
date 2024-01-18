from datetime import datetime, timedelta
from airflow.decorators import dag,task
from task_groups.process_tasks import process_tasks
from typing import Dict

@task.python(task_id="extract_subdags",do_xcom_push=False)
def extract() -> Dict[str, str]:
    partner_name = "netflix"
    partner_path = "partners/netflix"
    return {"partner_name": partner_name, "partner_path": partner_path}


default_args = {
    "start_date": datetime(2023, 1, 1),
}
@dag(
    schedule="@once",
    default_args=default_args,
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
    tags=["DE"],
    max_active_runs=1
    )
def my_dag_taskgroup():
    partner_settings = extract()
    process_tasks(partner_settings)
    

my_dag_taskgroup()

