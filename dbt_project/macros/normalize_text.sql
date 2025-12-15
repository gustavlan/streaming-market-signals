{% macro normalize_text(column_name) %}
-- Lowercase, strip punctuation, collapse whitespace (for track/artist matching)
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
    -- Trim leading/trailing whitespace
    '^\s+|\s+$', '', 'g'
)
{% endmacro %}
