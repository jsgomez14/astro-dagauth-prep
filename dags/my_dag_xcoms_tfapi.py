import json
from datetime import datetime, timedelta
from airflow.decorators import dag,task
from airflow.operators.python import PythonOperator
from typing import Dict

@task.python(do_xcom_push=False)
def extract() -> Dict[str, str]:
    partner_name = "netflix"
    partner_path = "partners/netflix"
    return {"partner_name": partner_name, "partner_path": partner_path}

@task.python
def process(partner_name, partner_path):
    print('partner_name',partner_name, 'partner_path',partner_path)

@dag(
    schedule="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
    tags=["DE"],
    max_active_runs=1
    )
def my_dag_xcoms_tfapi():
    partner_settings = extract()
    process(partner_settings['partner_name'], partner_settings['partner_path'])

my_dag_xcoms_tfapi()

