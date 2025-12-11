import os
import duckdb
import spotipy

CLIENT_ID = os.getenv("SPOTIPY_CLIENT_ID")
CLIENT_SECRET = os.getenv("SPOTIPY_CLIENT_SECRET")

BASE_DIR = "/opt/airflow/data"
DB_PATH = os.path.join(BASE_DIR, "music_warehouse.duckdb")

def enrich_artist():
    if not CLIENT_ID or not CLIENT_SECRET:
        raise ValueError("Spotify CLIENT_ID and CLIENT_SECRET must be set as environment variables.")
    
    print("Connecting to DuckDB...")
    con = duckdb.connect(DB_PATH)

    # Initialize Spotipy client, ClientCredentials is used since only public data is needed
    auth_manager = spotipy.oauth2.SpotifyClientCredentials(client_id=CLIENT_ID, client_secret=CLIENT_SECRET)
    sp = spotipy.Spotify(auth_manager=auth_manager)

    # Find arist/track IDs that are missing mapping
    print("Finding artists missing label mapping...")

    con.execute("""
        CREATE TABLE IF NOT EXISTS dim_track_metadata (
            track_name VARCHAR,
            artist_name VARCHAR,
            spotify_label VARCHAR,
            spotify_track_id VARCHAR,
            updated_at TIMESTAMP
        );
    """)

    query = """
        SELECT DISTINCT c.track_name, c.artist_name,
        FROM stg_combined_charts c
        LEFT JOIN dim_track_metadata m
            ON c.track_name = m.track_name AND c.artist_name = m.artist_name
        WHERE m.spotify_label IS NULL
        LIMIT 50 -- Limit to 50 for safety testing purposes
    """

    missing_tracks = con.sql(query).df

    if missing_tracks.empty:
        print("No new tracks to enrich.")
        return
    print(f"Found {len(missing_tracks)} tracks to enrich, connecting to spotify API...")

    results = []

    for _, row in missing_tracks.iterrows():