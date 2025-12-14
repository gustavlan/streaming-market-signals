with daily_scrape as (
    select * from {{ ref('daily_charts') }}
),

cleaned as (
    select
        chart_date,
        -- Kworb combines "Artist - Title", so we split them here.
        -- Logic: Take everything before the first " - " as Artist
        trim(split_part(artist_and_title, ' - ', 1)) as artist_name,
        -- Logic: Take everything after the first " - " as Track
        trim(substring(artist_and_title, length(split_part(artist_and_title, ' - ', 1)) + 4)) as track_name,
        streams as daily_streams,
        extracted_at
    from daily_scrape
)

select * from cleaned
