from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

with DAG(dag_id='trigger_airbyte_job_example',
         default_args={'owner': 'airflow'},
         schedule_interval='@daily',
         start_date=days_ago(1)
    ) as dag:

    money_to_json = AirbyteTriggerSyncOperator(
        task_id='airbyte_example',
        airbyte_conn_id='airbyte_conn',
        connection_id='3a0e80d8-1477-4dc4-b493-8356ef05ea67',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )