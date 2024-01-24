from airflow.decorators import (
    dag,
    task
)
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable
from datetime import datetime, timedelta

class CustomPostgresOperator(PostgresOperator):
    template_fields = ("sql", "parameters")

@dag(
    "my_dag",
    description = "DAG in charge of processing customer data.",
    start_date = datetime(2021, 1, 1),
    schedule = "@daily",
    dagrun_timeout = timedelta(minutes = 10),
    tags = ["data_engineering"],
    catchup = False,
    )
def my_dag():
    @task()
    def extract(partner_name):

        print("test variables:", partner_name)
    # Variable.get("my_dag_partner", deserialize_json= True)["name"]
    fetching_data = CustomPostgresOperator(
        task_id = "fetching_data",
        sql = "sql/MY_REQUEST.sql",
        parameters = {
            "next_ds": "{{  next_ds }}",
            "prev_ds":  "{{ prev_ds }}",
            "partner_name": "{{ var.json.my_dag_partner.name }}",

        },
    )
    first_step = extract("{{ var.json.my_dag_partner.name }}")
    parallel_step = fetching_data
    
my_dag()


