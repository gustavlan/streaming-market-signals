{{ config(
    materialized='incremental',
    unique_key='chart_date || parent_group',
    on_schema_change='append_new_columns'
) }}

-- Big 3 pattern matching macro for direct Spotify label strings
-- This catches labels like "pgLang, under exclusive license to Interscope Records"
{% set umg_patterns = [
    'universal', 'interscope', 'def jam', 'republic', 'capitol', 'geffen', 
    'island', 'polydor', 'verve', 'emi', 'motown', 'virgin', 'harvest',
    'umg', 'umc', 'ume', 'mca', 'decca', 'mercury', 'am records', 'a&m'
] %}
{% set sony_patterns = [
    'sony', 'columbia', 'epic', 'rca', 'arista', 'legacy', 'century media',
    'provident', 'jive', 'zomba', 'bmg', 'the orchard', 'kemosabe'
] %}
{% set warner_patterns = [
    'warner', 'atlantic', 'elektra', 'parlophone', 'reprise', 'rhino',
    'sire', 'wea', 'nonesuch', 'roadrunner', 'fueled by ramen', '300 entertainment',
    'big beat', 'east west', 'asylum', 'loma vista'
] %}

with charts as (
    select * from {{ ref('int_charts_with_track_id') }}
    {% if is_incremental() %}
    where chart_date > (select coalesce(max(chart_date), '1900-01-01') from {{ this }})
    {% endif %}
),

track_metadata as (
    select
        spotify_track_id,
        normalized_label,
        -- Keep original label for pattern matching
        spotify_label as original_label
    from {{ ref('stg_track_metadata_normalized') }}
),

label_hierarchy as (
    select
        normalized_label,
        market_share_group,
        ultimate_parent_name
    from {{ ref('int_labels_normalized') }}
),

joined as (
    select
        c.chart_date,
        c.track_name,
        c.artist_name,
        c.daily_streams,
        c.spotify_track_id,
        m.normalized_label as spotify_normalized_label,
        m.original_label,
        h.market_share_group as hierarchy_group,
        h.ultimate_parent_name,
        -- Direct pattern matching on Spotify label string (catches complex label strings)
        case
            {% for pattern in umg_patterns %}
            when lower(m.original_label) like '%{{ pattern }}%' then 'Universal Music Group'
            {% endfor %}
            {% for pattern in sony_patterns %}
            when lower(m.original_label) like '%{{ pattern }}%' then 'Sony Music Entertainment'
            {% endfor %}
            {% for pattern in warner_patterns %}
            when lower(m.original_label) like '%{{ pattern }}%' then 'Warner Music Group'
            {% endfor %}
            else null
        end as direct_match_group
    from charts c
    left join track_metadata m
        on c.spotify_track_id = m.spotify_track_id
    left join label_hierarchy h
        on m.normalized_label = h.normalized_label
),

resolved as (
    select
        chart_date,
        track_name,
        artist_name,
        daily_streams,
        spotify_track_id,
        -- Priority: 1) Direct pattern match, 2) Hierarchy match, 3) Unmapped
        coalesce(
            direct_match_group,
            case when hierarchy_group = 'Independent / Other' then null else hierarchy_group end,
            hierarchy_group,
            'Independent / Unmapped'
        ) as parent_group
    from joined
),

aggregated as (
    select
        chart_date,
        parent_group,
        sum(daily_streams) as total_streams,
        count(distinct spotify_track_id) as track_count,
        count(*) as row_count
    from resolved
    group by 1, 2
),

with_share as (
    select
        chart_date,
        parent_group,
        total_streams,
        track_count,
        row_count,
        total_streams::double / nullif(sum(total_streams) over (partition by chart_date), 0) as market_share_pct
    from aggregated
)

select * from with_share
order by chart_date desc, total_streams desc
