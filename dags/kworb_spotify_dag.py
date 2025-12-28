from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator, ShortCircuitOperator
from datetime import datetime, timedelta
import duckdb
import os

# Configuration
DB_PATH = '/opt/airflow/data/music_warehouse.duckdb'
SCRIPTS_DIR = '/opt/airflow/scripts'

def load_kworb_to_duckdb():
    """
    Loads the latest Kworb parquet files into the staging table 
    so the enrichment script can query them.
    """
    con = duckdb.connect(DB_PATH)
    # Create the view or table expected by enrich_metadata.py
    con.execute("""
        CREATE OR REPLACE TABLE stg_combined_charts AS 
        SELECT * FROM read_parquet('/opt/airflow/data/raw/kworb/*.parquet')
    """)
    con.close()

def check_if_enrichment_needed():
    """
    Returns True if there are tracks in staging that are missing from the metadata table.
    """
    con = duckdb.connect(DB_PATH)
    
    # Ensure metadata table exists
    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_track_metadata (
            track_name VARCHAR,
            artist_name VARCHAR,
            spotify_label VARCHAR,
            spotify_track_id VARCHAR,
            updated_at TIMESTAMP
        );
    """)
    
    # Logic mirrored from enrich_metadata.py
    query = """
        SELECT COUNT(*) 
        FROM stg_combined_charts c
        LEFT JOIN dim_track_metadata m
            ON c.track_name = m.track_name AND c.artist_name = m.artist_name
        WHERE m.spotify_label IS NULL
    """
    try:
        count = con.sql(query).fetchone()[0]
    except Exception as e:
        print(f"Error checking enrichment need: {e}")
        count = 0
    finally:
        con.close()
    
    print(f"Found {count} tracks requiring enrichment.")
    return count > 0

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'music_market_share_pipeline',
    default_args=default_args,
    schedule_interval='@daily',
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # 1. Scrape Kworb Data
    scrape_kworb = BashOperator(
        task_id='scrape_kworb',
        bash_command=f'python {SCRIPTS_DIR}/scrape_kworb.py',
        env={'DATA_DIR': '/opt/airflow/data'}
    )

    # 2. Load Parquet to DuckDB (Required for enrichment script to see the data)
    load_data = PythonOperator(
        task_id='load_kworb_to_duckdb',
        python_callable=load_kworb_to_duckdb
    )

    # 3. Smart Trigger to check if need to run the expensive API calls
    check_enrichment = ShortCircuitOperator(
        task_id='check_enrichment_needed',
        python_callable=check_if_enrichment_needed
    )

    # 4. Run Spotify Enrichment if check_enrichment returns True
    enrich_metadata = BashOperator(
        task_id='enrich_metadata',
        bash_command=f'python {SCRIPTS_DIR}/enrich_metadata.py',
        env={
            'DATA_DIR': '/opt/airflow/data',
            'SPOTIPY_CLIENT_ID': os.getenv('SPOTIPY_CLIENT_ID'),
            'SPOTIPY_CLIENT_SECRET': os.getenv('SPOTIPY_CLIENT_SECRET')
        }
    )

    scrape_kworb >> load_data >> check_enrichment >> enrich_metadata
