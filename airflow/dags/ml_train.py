# type: ignore
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id='ml_weekly_retrain',
    default_args={'owner': 'airflow', 'retries': 1,
                  'retry_delay': timedelta(minutes=5)},
    schedule_interval='0 7 * * 1',  # every Monday at 7am
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    BashOperator(
        task_id='run_ml_train',
        bash_command='python /opt/airflow/ml/train.py'
    )