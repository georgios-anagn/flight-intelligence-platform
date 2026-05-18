# type: ignore
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id='spark_feature_engineering',
    default_args={'owner': 'airflow', 'retries': 1,
                  'retry_delay': timedelta(minutes=5)},
    schedule_interval='30 6 * * *',  # 30 min after dbt runs
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:
    BashOperator(
        task_id='run_spark_features',
        bash_command='python /opt/airflow/spark/feature_engineering.py'
    )