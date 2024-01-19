from datetime import datetime, timedelta
from airflow.decorators import dag,task
from task_groups.process_tasks import process_tasks
from typing import Dict

partners = {
    "partner_snowflake" : { 
            "name" : "snowflake",
            "path" : "parnets/snowflake"
        },
    "partner_netflix" : {
            "name" : "netflix",
            "path" : "parnets/netflix"
        },
    "partner_amazon" : {
            "name" : "amazon",
            "path" : "parnets/amazon"
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
def my_dag_dynamic():

    for partner, details in partners.items():
        @task.python(task_id=f"extract_{partner}",do_xcom_push=False)
        def extract(partner_name, partner_path) -> Dict[str, str]:
            return {"partner_name": partner_name, "partner_path": partner_path}
        process_tasks(extract(details['name'], details['path']))
    
my_dag_dynamic()

