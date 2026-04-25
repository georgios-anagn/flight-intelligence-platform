# type: ignore
from airflow import DAG
from airflow.operators.bash import BashOperator 
from datetime import datetime, timedelta

with DAG( 
    dag_id='dbt_daily_run',
    default_args={'owner':'airflow','retries':1, 
                  'retry_delay':timedelta(minutes=5)},
    schedule_interval='0 6 * * *', 
    start_date=datetime(2024,1,1), 
    catchup=False
) as dag: 
    BashOperator( 
        task_id='run_dbt', 
        bash_command='cd dbt && dbt run' 
    )