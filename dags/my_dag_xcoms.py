import json
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator


def _extract(ti):
    partner_name = "netflix"
    partner_path = "partners/netflix"
    # ti.xcom_push(key="partner_name", value=partner_name)
    return {"partner_name": partner_name, "partner_path": partner_path}

def _process(ti):
    partner_attributes = ti.xcom_pull(task_ids="extract")
    print('here',partner_attributes)

with DAG(
    "my_dag_xcoms",
    schedule="@once",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=10),
    tags=["DE"],
    max_active_runs=1
    ) as dag:

    extract = PythonOperator(
        task_id="extract",
        python_callable=_extract
    )

    process = PythonOperator(
        task_id="process",
        python_callable=_process
    )

    extract >> process


