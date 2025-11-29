import duckdb
import os

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(BASE_DIR, 'data', 'music_warehouse.duckdb')
JSON_FILE_PATH = os.path.join(BASE_DIR, 'data', 'raw', 'musicbrainz', 'mbdump', 'label')

def load_labels_to_duckdb():
    print(f"🦆 Connecting to DuckDB at {DB_PATH}...")
    con = duckdb.connect(DB_PATH)
    
    print(f"🔎 Looking for file at: {JSON_FILE_PATH}")
    
    if not os.path.exists(JSON_FILE_PATH):
        raise FileNotFoundError(f"❌ Could not find MusicBrainz dump at {JSON_FILE_PATH}")

    print("⏳ Loading Label Data (Memory Safe Mode)...")
    
    con.execute(f"""
        CREATE OR REPLACE TABLE raw_musicbrainz_labels AS
        SELECT 
            id as mb_id, 
            name, 
            "label-code" as label_code,
            relations
        FROM read_json('{JSON_FILE_PATH}', 
            columns={{
                'id': 'VARCHAR', 
                'name': 'VARCHAR', 
                'label-code': 'INTEGER', 
                'relations': 'JSON',
                'type': 'VARCHAR'
            }},
            maximum_object_size=20000000
        );
    """)
    
    count = con.execute("SELECT count(*) FROM raw_musicbrainz_labels").fetchone()[0]
    print(f"✅ Successfully loaded {count:,} labels into 'raw_musicbrainz_labels'.")
    
    con.close()

if __name__ == "__main__":
    load_labels_to_duckdb()