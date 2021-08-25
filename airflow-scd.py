from airflow import DAG
from datetime import datetime, timedelta
from cloudera.cdp.airflow.operators.cde_operator import CDEJobRunOperator
from cloudera.cdp.airflow.operators.cdw_operator import CDWOperator
from airflow.operators.dummy_operator import DummyOperator

##run pip install apache-airflow
##pip install https://github.com/cloudera/cloudera-airflow-plugins/releases/download/v1.0.0/cloudera_airflow_provider-1.0.0-py3-none-any.whl

##execute via cli
###cde job run --config c_stageTable='product_staged' --config c_sourceLoc='s3a://se-uat2/sunman/product_changes/' --config c_stageCleansedTable=product_staged_cleansed  --config c_dimTable=product --name airflow job


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
    'cde-airflow-dag', default_args=default_args, catchup=False, is_paused_upon_creation=False)


hive_merge = """
MERGE INTO product a 
using product_staged_cleansed b 
ON ( a.product_id = b.product_id and a.is_current='Y') 
WHEN matched THEN UPDATE SET is_current = 'N', end_date = current_date();
MERGE INTO product a 
using product_staged_cleansed b 
ON ( a.product_id = b.product_id and a.product_name = b.product_name and a.aisle_id = b.aisle_id and a.department_id = b.department_id) 
WHEN NOT matched THEN INSERT VALUES (b.product_id, b.product_name, b.aisle_id, b.department_id, current_date(), null, 'Y');
"""


start = DummyOperator(task_id='start', dag=dag)


process_scd_staging = CDEJobRunOperator(
    task_id='cde_spark_job',
    dag=dag,
    job_name='StageSCD2',
    variables={
        'stageTable': "{{ dag_run.conf['c_stageTable'] }}",
        'sourceLoc': "{{ dag_run.conf['c_sourceLoc'] }}",
        'stageCleansedTable': "{{ dag_run.conf['c_stageCleansedTable'] }}",
        'dimTable': "{{ dag_run.conf['c_dimTable'] }}",
    }


)

##https://docs.cloudera.com/data-engineering/cloud/manage-jobs/topics/cde-airflow-dag-pipeline.html
hive_cdw_job = CDWOperator(
    task_id='cdw_hive_job',
    dag=dag,
    cli_conn_id='hive-vw',
    hql=hive_merge,
    schema='default',
    use_proxy_user=False,
    query_isolation=False

)



end = DummyOperator(task_id='end', dag=dag)

start >> process_scd_staging >> hive_cdw_job >> end