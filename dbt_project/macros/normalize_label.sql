{% macro normalize_label(column_name) %}
-- Lowercase, strip punctuation, collapse whitespace, remove common suffixes
regexp_replace(
    regexp_replace(
        regexp_replace(
            regexp_replace(
                lower(trim({{ column_name }})),
                -- Remove punctuation except spaces
                '[^a-z0-9 ]', '', 'g'
            ),
            -- Collapse multiple spaces to single space
            ' +', ' ', 'g'
        ),
        -- Remove common suffixes (order matters: longer first)
        '\s*(entertainment|recordings|recording|records|limited|company|music|group|gmbh|inc|llc|ltd|plc|co|sa|bv|ab)\s*$',
        '',
        'g'
    ),
    -- Trim again after suffix removal and collapse spaces
    '^\s+|\s+$', '', 'g'
)
{% endmacro %}
