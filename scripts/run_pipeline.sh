#!/usr/bin/env bash
set -euo pipefail

echo "=== Step 1: Scrape Kworb charts ==="
python scripts/scrape_kworb.py

echo "=== Step 2: Load MusicBrainz data ==="
python scripts/load_musicbrainz.py

echo "=== Step 3: Build label hierarchy ==="
python scripts/build_hierarchy.py

echo "=== Step 4: Create stg_combined_charts view ==="
python -c "
import duckdb, os
db = duckdb.connect(os.environ['DUCKDB_PATH'])
db.execute(\"CREATE OR REPLACE VIEW stg_combined_charts AS SELECT * FROM read_parquet('\" + os.environ['KWORB_PARQUET_GLOB'] + \"')\")
db.close()
"

echo "=== Step 5: Enrich metadata via Spotify ==="
if [ -z "${SPOTIPY_CLIENT_ID:-}" ]; then
  echo "WARNING: SPOTIPY_CLIENT_ID is unset, skipping Spotify enrichment"
else
  python scripts/enrich_metadata.py
fi

echo "=== Step 6: Run dbt transformations ==="
cd dbt_project && dbt deps && dbt run --target ci && cd ..

echo "=== Step 7: Fetch financial data ==="
python scripts/fetch_financials.py

echo "=== Step 8: Execute analysis notebook ==="
jupyter nbconvert --to notebook --execute notebooks/market_share_alpha.ipynb \
  --output market_share_alpha.ipynb \
  --ExecutePreprocessor.timeout=600

echo "=== Pipeline complete ==="
