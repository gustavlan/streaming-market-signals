{{ config(materialized='view') }}

with raw_metadata as (
    select * from {{ source('spotify_enrichment', 'dim_track_metadata') }}
)

select
    track_name,
    artist_name,
    spotify_label,
    spotify_track_id,
    updated_at,
    -- Normalized keys for matching
    {{ normalize_text('track_name') }} as normalized_track_name,
    {{ normalize_text('artist_name') }} as normalized_artist_name,
    {{ normalize_label('spotify_label') }} as normalized_label
from raw_metadata
where spotify_track_id is not null
