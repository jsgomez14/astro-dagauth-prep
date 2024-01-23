from airflow.decorators import (
    dag,
    task
)
from airflow.operators.dummy_operator import DummyOperator
from airflow.models.baseoperator import cross_downstream, chain
from airflow.models import Variable
from datetime import datetime, timedelta


@dag(
    description = "DAG in charge of processing customer data.",
    start_date = datetime(2021, 1, 1),
    schedule = "@once",
    dagrun_timeout = timedelta(minutes = 10),
    tags = ["data_engineering"],
    catchup = False,
    )
def my_dag_dependencies_helpers():
    t1 = DummyOperator(task_id="t1")
    t2 = DummyOperator(task_id="t2")
    t3 = DummyOperator(task_id="t3")
    t4 = DummyOperator(task_id="t4")
    t5 = DummyOperator(task_id="t5")
    t6 = DummyOperator(task_id="t6")
    t7 = DummyOperator(task_id="t7")

    # t2.set_upstream(t1) t2 << t1
    # t1.set_downstream(t2) t1 >> t2

    t8 = DummyOperator(task_id="t8")
    t9 = DummyOperator(task_id="t9")
    t10 = DummyOperator(task_id="t10")
    t11 = DummyOperator(task_id="t11")
    t12 = DummyOperator(task_id="t12")
    t13 = DummyOperator(task_id="t13")

    cross_downstream([t1, t2, t3],[t4, t5, t6])
    [t4, t5, t6] >> t7
    chain(t7, [t8, t9], [t10, t11], t12)
    t12 >> t13

my_dag_dependencies_helpers()


