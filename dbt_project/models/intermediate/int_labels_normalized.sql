{{ config(materialized='view') }}

with labels as (
    select * from {{ source('python_models', 'dim_labels') }}
)

select
    label_id,
    label_name,
    ultimate_parent_name,
    market_share_group,
    -- Normalized key for matching against Spotify labels
    {{ normalize_label('label_name') }} as normalized_label
from labels
