from airflow.decorators import dag,task
from airflow.operators.python import get_current_context

@task.python
def process_a():
    ti = get_current_context()['ti']
    partner_name = ti.xcom_pull(key='partner_name', task_ids='extract', dag_id='my_dag_subdags')
    partner_path = ti.xcom_pull(key='partner_path', task_ids='extract', dag_id='my_dag_subdags')
    print('partner_name',partner_name, 'partner_path',partner_path)

@task.python
def process_b():
    ti = get_current_context()['ti']
    partner_name = ti.xcom_pull(key='partner_name', task_ids='extract_subdags', dag_id='my_dag_subdags')
    partner_path = ti.xcom_pull(key='partner_path', task_ids='extract_subdags', dag_id='my_dag_subdags')
    print('partner_name',partner_name, 'partner_path',partner_path)

@task.python
def process_c():
    ti = get_current_context()['ti']
    partner_name = ti.xcom_pull(key='partner_name', task_ids='extract_subdags', dag_id='my_dag_subdags')
    partner_path = ti.xcom_pull(key='partner_path', task_ids='extract_subdags', dag_id='my_dag_subdags')
    print('partner_name',partner_name, 'partner_path',partner_path)

def subdag_factory(parent_dag_id, subdag_dag_id, default_args):
    @dag(dag_id = f"{parent_dag_id}.{subdag_dag_id}",
         default_args=default_args)
    def subdag():
        process_a()
        process_b()
        process_c()
        #TODO: Subdag tasks are unable to pull xcoms from parent dag. Need to fix this.
    
    return subdag()
