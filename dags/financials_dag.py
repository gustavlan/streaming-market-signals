from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'music_financials_daily',
    default_args=default_args,
    description='Fetch daily stock prices for Big 3 Music Labels',
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['financials', 'yahoo'],
) as dag:

    fetch_stocks = BashOperator(
        task_id='fetch_stock_prices',
        bash_command='python /opt/airflow/scripts/fetch_financials.py',
        env={
            'DATA_DIR': '/opt/airflow/data',
        }
    )
