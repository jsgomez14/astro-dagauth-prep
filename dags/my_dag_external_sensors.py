from airflow.decorators import task, dag
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator
    
from datetime import datetime, timedelta 

dag_owner = 'Yanse'

default_args = {'owner': dag_owner,
        'depends_on_past': False,
        'retries': 2,
        'retry_delay': timedelta(minutes=5)
        }

@dag(
        default_args=default_args,
        description='External sensor theory',
        start_date=datetime(2022, 1, 1),
        schedule_interval='@daily',
        catchup=False,
        tags=['DE']
)
def my_dag_external_sensors():
    start = EmptyOperator(task_id='start')

    waiting_for_task = ExternalTaskSensor(
        task_id="waiting_for_task",
        external_dag_id="my_dag",
        external_task_id="extract",
        # execution_delta=timedelta(minutes=0), Delta between the current 
        # execution date and the execution date of the external task
        # execution_date_fn = function that returns the expected execution dates for the sensor.
        # failed_states = ['failed', 'skipped'] fail if expected task failed or skipped.
        # allowd_states = ['success'] allowd states for the external task.
    )

    cleaning_xcoms = PostgresOperator(
    task_id='cleaning_xcoms',
    postgres_conn_id="postgres",
    sql='sql/CLEANING_XCOMS.sql',
    )

    start >> waiting_for_task >> cleaning_xcoms

my_dag_external_sensors()