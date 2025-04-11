from airflow import DAG
from airflow.utils.db import provide_session
from airflow.models import TaskInstance
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.operators.python import PythonOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1, 22, 0, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def create_cloud_run_task(task_id, command, target, execution_date=None):
    """
    Crée une tâche Cloud Run sans inclure execution_date si elle est None ou "None".
    """
    args = [command]

    if execution_date not in [None, "None", "", 'None']:
        args.append(execution_date)

    print(f"[DEBUG] {task_id} envoyé avec args={args}")  # Debugging final

    return CloudRunExecuteJobOperator(
        task_id=f"{task_id}_{target}",
        project_id='fournisseur-data',
        region='europe-west9',
        job_name='scrappers-uat-job',
        overrides={
            "container_overrides": [{
                "args": args
            }]
        },
        gcp_conn_id='google_cloud_default'
    )

def create_cloud_run_task_dbt(task_id, command, target, exclude_model=None):
    args = [command]
    if exclude_model:
        args.extend(["--exclude", exclude_model])
        
    return CloudRunExecuteJobOperator(
        task_id=f"{task_id}_{target}",
        project_id='fournisseur-data',
        region='europe-west9', 
        job_name='dbt-airflow-job-uat',
        overrides={
            "container_overrides": [{
                "args": args
            }]
        },
        gcp_conn_id='google_cloud_default'
    )

with DAG('pharma_data_full_pipeline_uat',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    save_sitemaps_links_to_mongo = create_cloud_run_task(
        "save_sitemaps_links_to_mongo", 
        "modules.scrappers.save_sitemaps_links_to_mongo", 
        "uat")
    
    airbyte_mongo_to_postgre = AirbyteTriggerSyncOperator(
        task_id='airbyte_mongo_to_postgre_uat',
        airbyte_conn_id='airbyte_conn',
        connection_id='09d58bc3-c83e-4c12-9a31-887398486862',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    run_pharma_scrapper = create_cloud_run_task(
        "pharma_scrapper",
        "modules.scrappers.pharma_scrapper",
        "uat"
    )

    dbt_seed_uat = create_cloud_run_task_dbt("seed", "seed", "uat")
    dbt_snapshot_uat = create_cloud_run_task_dbt("snapshot", "snapshot", "uat")
    dbt_run_uat = create_cloud_run_task_dbt("run", "run", "uat", exclude_model="staging.curated.stg_similarity_scores_categorie_taxonomy")
    dbt_test_uat = create_cloud_run_task_dbt("test", "test", "uat")

    run_save_images = create_cloud_run_task(
        "run_save_images", 
        "modules.download_images.save_to_gcs", 
        "uat")
    
    airbyte_postgre_to_firestore = AirbyteTriggerSyncOperator(
        task_id='airbyte_postgre_to_firestore_uat',
        airbyte_conn_id='airbyte_conn',
        connection_id='ed28a48d-e6d4-41f0-8544-7eeaf2e0d30b"',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    stop_vm_task = CloudRunExecuteJobOperator(
    task_id="stop_uat_vm",
    project_id='fournisseur-data',
    region='europe-west9',
    job_name='stop-vm-job',
    gcp_conn_id='google_cloud_default',
    trigger_rule='all_done'
    )

    save_sitemaps_links_to_mongo >> run_pharma_scrapper >> airbyte_mongo_to_postgre >> dbt_seed_uat >> dbt_snapshot_uat >> dbt_run_uat >> dbt_test_uat >> run_save_images >> airbyte_postgre_to_firestore >> stop_vm_task
