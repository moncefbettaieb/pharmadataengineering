from airflow import DAG
from airflow.utils.db import provide_session
from airflow.models import TaskInstance
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1, 22, 0, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@provide_session
def get_last_pharma_scrapper_success(session=None, **context):
    """
    Récupère la dernière exécution réussie du task 'pharma_scrapper_prod', soustrait 3 jours.
    Retourne une date au format '%d-%m-%Y' ou None si aucune exécution réussie.
    """
    try:
        # Récupérer la dernière exécution réussie du task
        result = (
            session.query(TaskInstance.start_date)  # Utilisation de `start_date` au lieu de `execution_date`
            .filter(
                TaskInstance.dag_id == "crawler_pipeline",
                TaskInstance.task_id == "pharma_scrapper_prod",
                TaskInstance.state == "success"
            )
            .order_by(TaskInstance.start_date.desc())  # Trier par la plus récente exécution
            .limit(1)
            .scalar()
        )

        if result:
            date_str = (result - timedelta(days=3)).strftime("%d-%m-%Y")
            print(f"[DEBUG] Dernière exécution réussie trouvée : {date_str}")
            return date_str

        print("[DEBUG] Aucune exécution trouvée, retour de None")
        return None  

    except Exception as e:
        print(f"[ERROR] Problème SQLAlchemy : {e}")
        return None

def clean_execution_date(**kwargs):
    """Récupère execution_date depuis XCom et assure que "None" devient None."""
    execution_date = kwargs['ti'].xcom_pull(task_ids='get_last_pharma_scrapper_success')

    if execution_date in [None, "None", "", 'None']:
        print("[DEBUG] execution_date est vide ou 'None', retour None")
        return None  

    print(f"[DEBUG] execution_date après nettoyage : {execution_date}")
    return execution_date

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
        job_name='scrappers-prod-job',
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
        job_name='dbt-airflow-job-prod',
        overrides={
            "container_overrides": [{
                "args": args
            }]
        },
        gcp_conn_id='google_cloud_default'
    )

with DAG('pharma_data_full_pipeline_prod',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    get_exec_date = PythonOperator(
        task_id="get_last_pharma_scrapper_success_prod",
        python_callable=get_last_pharma_scrapper_success,
        provide_context=True
    )

    clean_exec_date = PythonOperator(
        task_id="clean_execution_date_prod",
        python_callable=clean_execution_date,
        provide_context=True
    )

    execution_date = "{{ ti.xcom_pull(task_ids='clean_execution_date') }}"

    save_sitemaps_links_to_mongo = create_cloud_run_task(
        "save_sitemaps_links_to_mongo", 
        "modules.scrappers.save_sitemaps_links_to_mongo", 
        "prod")
    
    airbyte_mongo_to_postgre = AirbyteTriggerSyncOperator(
        task_id='airbyte_mongo_to_postgre_prod',
        airbyte_conn_id='airbyte_conn',
        connection_id='09d58bc3-c83e-4c12-9a31-887398486862',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    run_pharma_scrapper = create_cloud_run_task(
        "pharma_scrapper",
        "modules.scrappers.pharma_scrapper",
        "prod",
        execution_date=execution_date
    )

    dbt_seed_prod = create_cloud_run_task_dbt("seed", "seed", "prod")
    dbt_snapshot_prod = create_cloud_run_task_dbt("snapshot", "snapshot", "prod")
    dbt_run_prod = create_cloud_run_task_dbt("run", "run", "prod", exclude_model="staging.curated.int_similarity_scores_categorie_taxonomy")
    dbt_test_prod = create_cloud_run_task_dbt("test", "test", "prod")

    run_save_images = create_cloud_run_task(
        "run_save_images", 
        "modules.download_images.save_to_gcs", 
        "prod")
    
    airbyte_postgre_to_firestore = AirbyteTriggerSyncOperator(
        task_id='airbyte_postgre_to_firestore_prod',
        airbyte_conn_id='airbyte_conn',
        connection_id='ed28a48d-e6d4-41f0-8544-7eeaf2e0d30b"',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    save_sitemaps_links_to_mongo >> get_exec_date >> clean_exec_date >> run_pharma_scrapper >> airbyte_mongo_to_postgre >> dbt_seed_prod >> dbt_snapshot_prod >> dbt_run_prod >> dbt_test_prod >> run_save_images >> airbyte_postgre_to_firestore
