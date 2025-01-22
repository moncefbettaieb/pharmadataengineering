from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_cloud_run_task(task_id, command, target, exclude_model=None):
    args = [command]
    if exclude_model:
        args.extend(["--exclude", exclude_model])
        
    return CloudRunExecuteJobOperator(
        task_id=f"{task_id}_{target}",
        project_id='fournisseur-data',
        region='europe-west9', 
        job_name='dbt-airflow-job',
        overrides={
            "container_overrides": [{
                "args": args
            }]
        },
        gcp_conn_id='google_cloud_default'
    )

with DAG('dbt_full_pipeline',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:
    
    uat_seed = create_cloud_run_task("seed", "seed", "uat")
    uat_run = create_cloud_run_task("run", "run", "uat")
    uat_test = create_cloud_run_task("test", "test", "uat")

    uat_seed >> uat_run >> uat_test