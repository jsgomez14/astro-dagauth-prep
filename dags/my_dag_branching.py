from datetime import datetime, timedelta
from airflow.decorators import dag,task
from airflow.operators.python import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
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

def _choosing_partner_based_on_day(execution_date):
    day = execution_date.day_of_week
    print(day)
    if day == 1:
        return 'extract_partner_snowflake'
    elif day == 3:
        return 'extract_partner_netflix'
    elif day == 6:
        return 'extract_partner_amazon'
    else:
        return 'stop'


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
def my_dag_branching():
    # Default trigger_rule = all_success
    # trigger_rule = all_failed, all upstream tasks failed.
    # trigger_rule = one_failed, one upstream task failed.
    # trigger_rule = one_success, one upstream task success.
    # trigger_rule = none_failed, upstream tasks either skipped or succeded.
    # trigger_rule = none_failed, upstream tasks either skipped or succeded. Won't be triggered if one of the upstream tasks failed.
    # trigger_rule = none_failed_or_skipped, at least one of upstream tasks succeded and all the upstream tasks were executed.
    # trigger_rule = dummy, task gets triggered right away.


    choosing_partner_based_on_day = BranchPythonOperator(
        task_id="choosing_partner_based_on_day",
        python_callable=_choosing_partner_based_on_day
    )
    stop = DummyOperator(task_id="stop")
    choosing_partner_based_on_day >> stop
    storing = DummyOperator(task_id="storing", trigger_rule='none_failed_or_skipped')

    for partner, details in partners.items():
        @task.python(task_id=f"extract_{partner}",do_xcom_push=False)
        def extract(partner_name, partner_path) -> Dict[str, str]:
            return {"partner_name": partner_name, "partner_path": partner_path}
        extracted_values = extract(details['name'], details['path'])
        choosing_partner_based_on_day >> extracted_values
        process_tasks(extracted_values) >> storing

    
my_dag_branching()

