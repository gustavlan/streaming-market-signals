with source as (
    select * from {{ ref('stg_musicbrainz_labels') }}
),

flattened as (
    select 
        label_id as child_label_id,
        label_name as child_label_name,
        -- Unnest explodes the list one row per relationship
        unnest(CAST(relations AS JSON[])) as r
    from source
    where relations is not null
)

select
    child_label_id,
    child_label_name,
    -- What is the relationship? (e.g., subsidiary of, imprint of)
    r ->> 'type' as relationship_type,
    -- Forward usually means child -> parent, backward means parent -> child.
    r ->> 'direction' as direction,
    
    -- Extract parent info (Only exists if target type is label)
    r -> 'label' ->> 'id' as parent_label_id,
    r -> 'label' ->> 'name' as parent_label_name

from flattened
-- We only care about label-to-label links
where r ->> 'target-type' = 'label'