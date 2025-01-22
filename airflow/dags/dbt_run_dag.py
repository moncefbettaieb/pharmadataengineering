from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

with DAG(
    'dbt_run_dag',
    default_args=default_args,
    description='Run dbt models in production',
    schedule_interval=None,
    start_date=days_ago(1),
    catchup=False,
) as dag:

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /path/to/dbt/project && dbt run --target prod',
    )

    dbt_test = BashOperator(
        task_id='dbt_test',
        bash_command='cd /path/to/dbt/project && dbt test --target prod',
    )

    dbt_run >> dbt_test
