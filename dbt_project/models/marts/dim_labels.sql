{{ config(materialized='table') }}

-- MusicBrainz direction semantics:
--   'forward' = this label (child_label_id) OWNS the target (parent_label_id)
--   'backward' = target (parent_label_id) OWNS this label (child_label_id)
--
-- We want to traverse from subsidiary -> owner (following the ownership chain upward)
-- So we need to handle direction correctly in the join condition

with ownership_edges as (
    -- Normalize edges to always point from owned_label -> owner_label
    select
        case 
            when direction = 'forward' then parent_label_id  -- owned label
            else child_label_id  -- owned label
        end as owned_label_id,
        case 
            when direction = 'forward' then child_label_id   -- owner
            else parent_label_id  -- owner
        end as owner_label_id,
        case 
            when direction = 'forward' then parent_label_name
            else child_label_name
        end as owned_label_name,
        case 
            when direction = 'forward' then child_label_name
            else parent_label_name
        end as owner_label_name
    from {{ ref('int_label_relationships') }}
    where relationship_type in ('label ownership', 'imprint')
    and direction is not null
),

recursive hierarchy as (
    -- Anchor member: select all labels as starting points
    select 
        label_id,
        label_name,
        label_id as current_id,
        label_name as current_name,
        0 as depth,
        cast(label_name as varchar) as path
    from {{ ref('stg_musicbrainz_labels') }}

    union all

    -- Recursive member: follow ownership edges upward
    select 
        h.label_id,
        h.label_name,
        e.owner_label_id as current_id,
        e.owner_label_name as current_name,
        h.depth + 1 as depth,
        cast(h.path || ' > ' || e.owner_label_name as varchar) as path
    from hierarchy h
    join ownership_edges e on h.current_id = e.owned_label_id
    where h.depth < 10  -- Safety brake for cycles
),

ranked_hierarchy as (
    select
        *,
        row_number() over (partition by label_id order by depth desc) as rn
    from hierarchy
)

select
    label_id,
    label_name,
    current_name as ultimate_parent_name,
    path as ownership_path,
    case 
        when current_name ilike '%Universal Music%' then 'Universal Music Group'
        when current_name ilike '%Sony Music%' then 'Sony Music Entertainment'
        when current_name ilike '%Warner Music%' then 'Warner Music Group'
        else 'Independent / Other'
    end as market_share_group
from ranked_hierarchy
where rn = 1
where rn = 1
