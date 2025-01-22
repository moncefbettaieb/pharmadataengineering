from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunJobStartOperator
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
    description='Trigger Cloud Run jobs',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    run_job = CloudRunJobStartOperator(
        task_id="trigger_cloud_run_job",
        location="europe-west1",
        project_id="your-gcp-project-id",
        job_name="your-cloud-run-job",
        body={},  # Spécifie les paramètres si nécessaires
    )