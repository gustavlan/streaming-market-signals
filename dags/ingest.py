from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'music_market_share_ingest',
    default_args=default_args,
    description='Daily pipeline: scrape charts, enrich metadata via Spotify API, build dbt marts',
    schedule_interval='0 9 * * *',
    start_date=datetime(2025, 12, 18),
    catchup=True,
    tags=['music', 'ingestion', 'dbt'],
) as dag:

    scrape_kworb_task = BashOperator(
        task_id='scrape_kworb_data',
        bash_command='python /opt/airflow/scripts/scrape_kworb.py'
    )

    enrich_metadata_task = BashOperator(
        task_id='enrich_track_metadata',
        bash_command='python /opt/airflow/scripts/enrich_metadata.py'
    )

    build_dbt_models_task = BashOperator(
        task_id='build_dbt_models',
        bash_command=(
            'dbt run '
            '--project-dir /opt/airflow/dbt_project '
            '--profiles-dir /opt/airflow/dbt_project '
            '--select '
            'stg_combined_charts '
            'stg_track_metadata_normalized '
            'stg_dim_labels_normalized '
            'int_charts_with_track_id '
            'fact_market_share'
        ),
    )

    scrape_kworb_task >> enrich_metadata_task >> build_dbt_models_task