from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from datetime import datetime, timedelta
import logging
import requests
import smtplib
from email.mime.text import MIMEText

def slack_failure_alert(context):
    slack_webhook = BaseHook.get_connection('slack_alerts').password
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url

    message = f"""
    *:pill: PHARMA PIPELINE ALERT :rotating_light:*

    *❌ Échec détecté dans le DAG:* `{dag_id}`
    *🔧 Tâche:* `{task_id}`
    *🕒 Date d'exécution:* `{execution_date}`

    🔍 *Logs:* <{log_url}|Clique ici pour voir les logs>
    """
    requests.post(slack_webhook, json={"text": message})

def slack_success_alert(context):
    slack_webhook = BaseHook.get_connection('slack_alerts').password
    dag_id = context.get('dag').dag_id
    task_id = context.get('task_instance').task_id
    execution_date = context.get('execution_date')
    log_url = context.get('task_instance').log_url

    message = f"""
    :tada: *PHARMA PIPELINE - SUCCÈS !* :white_check_mark:

    *✅ Tâche:* `{task_id}` dans le DAG `{dag_id}`
    *🕒 Date:* `{execution_date}`

    📘 *Logs:* <{log_url}|Voir les logs>
    """
    requests.post(slack_webhook, json={"text": message})

def send_success_email(task_id, dag_id, execution_date):
    smtp_conn = BaseHook.get_connection("smtp_default")

    sender = smtp_conn.login
    recipient = "mbettaieb@gcdconsulting.fr"
    smtp_host = smtp_conn.host
    smtp_port = smtp_conn.port
    smtp_password = smtp_conn.password

    msg = MIMEText(f"La tâche `{task_id}` du DAG `{dag_id}` a réussi à {execution_date}.")
    msg['Subject'] = f"[Airflow] ✅ Succès: {dag_id}.{task_id}"
    msg['From'] = sender
    msg['To'] = recipient

    with smtplib.SMTP(smtp_host, smtp_port) as server:
        server.starttls()
        server.login(sender, smtp_password)
        server.send_message(msg)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023, 1, 1, 22, 0, 0),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email': ['mbettaieb@gcdconsulting.fr'],
    'email_on_failure': True,
    'email_on_retry': False
}

def log_args(task_id, args):
    logging.info(f"[DEBUG] {task_id} envoyé avec args={args}")

def on_pipeline_success(**context):
    slack_success_alert(context)
    task_id = context['task_instance'].task_id
    dag_id = context['dag'].dag_id
    execution_date = context['execution_date']
    send_success_email(task_id, dag_id, execution_date)

def create_cloud_run_operator(task_id, command, target, job_name, execution_date=None, exclude_model=None):
    args = [command]
    if execution_date not in [None, "None", "", 'None']:
        args.append(execution_date)
    if exclude_model:
        args.extend(["--exclude", exclude_model])
    
    log_args(task_id, args)
    
    return CloudRunExecuteJobOperator(
        task_id=f"{task_id}_{target}",
        project_id='fournisseur-data',
        region='europe-west9',
        job_name=job_name,
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
         catchup=False,
         on_failure_callback=slack_failure_alert) as dag:

    save_sitemaps_links_to_mongo = create_cloud_run_operator(
        "save_sitemaps_links_to_mongo", 
        "modules.scrappers.save_sitemaps_links_to_mongo", 
        "uat",
        job_name='scrappers-uat-job'
    )
    
    airbyte_mongo_to_postgre = AirbyteTriggerSyncOperator(
        task_id='airbyte_mongo_to_postgre_uat',
        airbyte_conn_id='airbyte_conn',
        connection_id='c2ba354e-8bda-4e8f-a128-f39f558253f5',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    run_pharma_scrapper = create_cloud_run_operator(
        "pharma_scraper",
        "modules.scrappers.pharma_scraper",
        "uat",
        job_name='scrappers-uat-job'
    )

    dbt_seed_uat = create_cloud_run_operator("seed", "seed", "uat", job_name='dbt-airflow-job-uat')
    dbt_snapshot_uat = create_cloud_run_operator("snapshot", "snapshot", "uat", job_name='dbt-airflow-job-uat')
    dbt_run_uat = create_cloud_run_operator("run", "run", "uat", job_name='dbt-airflow-job-uat', exclude_model="staging.curated.int_similarity_scores_categorie_taxonomy")
    dbt_test_uat = create_cloud_run_operator("test", "test", "uat", job_name='dbt-airflow-job-uat')

    run_save_images = create_cloud_run_operator(
        "run_save_images", 
        "modules.download_images.save_to_gcs", 
        "uat",
        job_name='scrappers-uat-job'
    )
    
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

    success_notify_task = PythonOperator(
    task_id='success_notify_uat',
    python_callable=on_pipeline_success,
    provide_context=True,
    trigger_rule='all_success'
)

    save_sitemaps_links_to_mongo >> run_pharma_scrapper >> airbyte_mongo_to_postgre >> dbt_seed_uat >> dbt_snapshot_uat >> dbt_run_uat >> dbt_test_uat >> run_save_images >> airbyte_postgre_to_firestore >> success_notify_task >> stop_vm_task
