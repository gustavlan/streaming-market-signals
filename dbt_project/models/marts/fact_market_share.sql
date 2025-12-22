{{ config(
    materialized='incremental',
    unique_key='chart_date || parent_group',
    on_schema_change='append_new_columns'
) }}

with charts as (
    select * from {{ ref('int_charts_with_track_id') }}
    {% if is_incremental() %}
    where chart_date > (select coalesce(max(chart_date), '1900-01-01') from {{ this }})
    {% endif %}
),

track_metadata as (
    select
        spotify_track_id,
        normalized_label
    from {{ ref('stg_track_metadata_normalized') }}
),

label_hierarchy as (
    select
        normalized_label,
        market_share_group,
        ultimate_parent_name
    from {{ ref('stg_dim_labels_normalized') }}
),

joined as (
    select
        c.chart_date,
        c.track_name,
        c.artist_name,
        c.daily_streams,
        c.spotify_track_id,
        m.normalized_label as spotify_normalized_label,
        coalesce(h.market_share_group, 'Independent / Unmapped') as parent_group,
        h.ultimate_parent_name
    from charts c
    left join track_metadata m
        on c.spotify_track_id = m.spotify_track_id
    left join label_hierarchy h
        on m.normalized_label = h.normalized_label
),

aggregated as (
    select
        chart_date,
        parent_group,
        sum(daily_streams) as total_streams,
        count(distinct spotify_track_id) as track_count,
        count(*) as row_count
    from joined
    group by 1, 2
),

with_share as (
    select
        chart_date,
        parent_group,
        total_streams,
        track_count,
        row_count,
        -- DOUBLE cast to avoid integer division
        total_streams::double / nullif(sum(total_streams) over (partition by chart_date), 0) as market_share_pct
    from aggregated
)

select * from with_share
order by chart_date desc, total_streams desc
