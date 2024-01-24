from airflow.decorators import task, dag
from airflow.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
    
from datetime import datetime, timedelta 

dag_owner = 'Yanse'

default_args = {'owner': dag_owner,
        'depends_on_past': False
        }

@dag(
        default_args=default_args,
        description='External sensor theory',
        start_date=datetime(2022, 1, 1),
        schedule_interval='@daily',
        catchup=False,
        tags=['DE']
)
def my_dag_trigger_dag():
    start = EmptyOperator(task_id='start')

    trigger_my_dag = TriggerDagRunOperator(
        task_id="trigger_my_dag",
        trigger_dag_id="my_dag",
        execution_date = '{{ ds }}',
        wait_for_completion=True,
        poke_interval=10,
        reset_dag_run = True, # Always set it to true
        failed_states = ['failed']
    )

    cleaning_xcoms = PostgresOperator(
    task_id='cleaning_xcoms',
    postgres_conn_id="postgres",
    sql='sql/CLEANING_XCOMS.sql',
    )

    start >> trigger_my_dag >> cleaning_xcoms

my_dag_trigger_dag()