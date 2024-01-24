from datetime import datetime, timedelta
from airflow.decorators import dag,task
from task_groups.process_tasks import process_tasks
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
def my_dag_pool():

    for partner, details in partners.items():
        @task.python(task_id=f"extract_{partner}",do_xcom_push=False,
            pool='partner_pool',
            pool_slots=1,
            priority_weight = details['priority'], # Only works for tasks within the same pool.
            # depends_on_past = True, If last execution failed, this execution will fail.
            ) 
        def extract(partner_name, partner_path) -> Dict[str, str]:
            time.sleep(5)
            return {"partner_name": partner_name, "partner_path": partner_path}
        process_tasks(extract(details['name'], details['path']))
    # Downstream, upstream, absolute priorities
my_dag_pool()

