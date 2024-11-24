from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta 

username = '12885f560a0b'
databricks_notebook_path = '/Users/davidjchampney@gmail.com/Pinterest Batch Data Notebook'
databricks_cluster_id = '1108-162752-8okw8dgg'

#Define params for Submit Run Operator
notebook_task = {
    'notebook_path': databricks_notebook_path,
}


#Define params for Run Now Operator
notebook_params = {
    "Variable":5
}


default_args = {
    'owner': username,
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3    ,
    'retry_delay': timedelta(minutes=2)
}


with DAG(f'{username}_dag',
    # should be a datetime format
    start_date=datetime(2024, 10, 27),
    # check out possible intervals, should be a string
    schedule_interval='0 0 * * *',
    catchup=False,
    default_args=default_args
    ) as dag:


    opr_submit_run = DatabricksSubmitRunOperator(
        task_id='submit_run',
        databricks_conn_id='databricks_default',
        existing_cluster_id=databricks_cluster_id,
        notebook_task=notebook_task
    )
    opr_submit_run
