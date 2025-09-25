{% macro normalize_sector(subparts_col) %}
  -- Map EPA subparts to broader sectors
  CASE
    WHEN {{ subparts_col }} IS NULL THEN 'Unknown'
    ELSE COALESCE(
      (SELECT ANY_VALUE(sector)
       FROM {{ ref('subpart_to_sector_map') }}
       WHERE {{ subparts_col }} ILIKE '%' || subpart || '%'),
      'Other'
    )
  END
{% endmacro %}
