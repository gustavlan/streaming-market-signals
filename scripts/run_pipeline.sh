#!/usr/bin/env bash
set -euo pipefail

# Default env vars (can be overridden by GHA workflow or .env)
export DBT_TARGET="${DBT_TARGET:-ci}"

# Resolve relative paths to absolute so they work after cd into dbt_project/
REPO_ROOT="$(pwd)"
_duckdb="${DUCKDB_PATH:-data/music_warehouse.duckdb}"
_parquet="${KWORB_PARQUET_GLOB:-data/raw/kworb/*.parquet}"
[[ "$_duckdb" != /* ]] && _duckdb="${REPO_ROOT}/${_duckdb}"
[[ "$_parquet" != /* ]] && _parquet="${REPO_ROOT}/${_parquet}"
export DUCKDB_PATH="$_duckdb"
export KWORB_PARQUET_GLOB="$_parquet"

# Resolve DBT_PROFILES_DIR to absolute path so dbt finds it after cd
_profiles="${DBT_PROFILES_DIR:-dbt_project}"
[[ "$_profiles" != /* ]] && _profiles="${REPO_ROOT}/${_profiles}"
export DBT_PROFILES_DIR="$_profiles"

echo "=== Step 1: Scrape Kworb charts ==="
python scripts/scrape_kworb.py

echo "=== Step 2: Load MusicBrainz data ==="
python scripts/load_musicbrainz.py

echo "=== Step 3: Build MusicBrainz dbt models ==="
cd dbt_project && dbt deps --quiet && dbt run --target "$DBT_TARGET" --select stg_musicbrainz_labels int_label_relationships && cd ..

echo "=== Step 4: Build label hierarchy ==="
python scripts/build_hierarchy.py

echo "=== Step 5: Bootstrap DuckDB views for enrichment ==="
python - <<'PYEOF'
import os, duckdb

db_path = os.environ["DUCKDB_PATH"]
parquet_glob = os.environ["KWORB_PARQUET_GLOB"]
con = duckdb.connect(db_path)

# Recreate the combined charts view with proper column transforms
# (enrich_metadata.py expects track_name, artist_name, daily_streams)
con.execute(f"""
    CREATE OR REPLACE VIEW stg_combined_charts AS
    WITH daily_scrape AS (
        SELECT * FROM read_parquet('{parquet_glob}')
    )
    SELECT
        chart_date,
        trim(split_part(artist_and_title, ' - ', 1)) AS artist_name,
        trim(substring(artist_and_title, length(split_part(artist_and_title, ' - ', 1)) + 4)) AS track_name,
        streams AS daily_streams,
        extracted_at
    FROM daily_scrape
""")

# Ensure dim_track_metadata exists (enrichment reads/writes it)
con.execute("""
    CREATE TABLE IF NOT EXISTS dim_track_metadata (
        track_name VARCHAR,
        artist_name VARCHAR,
        spotify_label VARCHAR,
        spotify_track_id VARCHAR,
        updated_at TIMESTAMP
    )
""")
con.close()
print("Bootstrap done: stg_combined_charts view + dim_track_metadata table ready.")
PYEOF

echo "=== Step 6: Enrich metadata via Spotify ==="
if [ -z "${SPOTIPY_CLIENT_ID:-}" ]; then
  echo "WARNING: SPOTIPY_CLIENT_ID is unset, skipping Spotify enrichment"
else
  python scripts/enrich_metadata.py
fi

echo "=== Step 7: Run full dbt transformations ==="
cd dbt_project && dbt run --target "$DBT_TARGET" && cd ..

echo "=== Step 8: Fetch financial data ==="
python scripts/fetch_financials.py

echo "=== Step 9: Execute analysis notebook ==="
jupyter nbconvert --to notebook --execute notebooks/market_share_alpha.ipynb \
  --output market_share_alpha.ipynb \
  --ExecutePreprocessor.timeout=600

echo "=== Pipeline complete ==="
