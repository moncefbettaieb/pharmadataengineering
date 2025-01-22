from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'cloud_run_dag',
    default_args=default_args,
    description='Run dbt job on Cloud Run',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    run_dbt_job = CloudRunExecuteJobOperator(
        task_id="execute_dbt_job",
        region='europe-west9',
        project_id='fournisseur-data',
        job_name="pharma-dbt-job",
        timeout_seconds=300,
        gcp_conn_id='google_cloud_default'
    )
