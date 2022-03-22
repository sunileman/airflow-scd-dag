from airflow import DAG
from datetime import datetime, timedelta
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
from cloudera.cdp.airflow.operators.cdw_operator import CDWOperator
from airflow.operators.dummy_operator import DummyOperator



default_args = {
    'owner': 'sunilemanjee',
    'depends_on_past': False,
    'email': ['sm@cloudera.com'],
    'start_date': datetime(2020,12,7,8),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'cde-airflow-dag',
    default_args=default_args,
    catchup=False,
    is_paused_upon_creation=False,
    max_active_runs=1
)


start = DummyOperator(task_id='start', dag=dag)

dagnode = CDEJobRunOperator(
    task_id='node1',
    dag=dag,
    job_name='simplejob'
)


end = DummyOperator(task_id='end', dag=dag)

start >> dagnode >> end