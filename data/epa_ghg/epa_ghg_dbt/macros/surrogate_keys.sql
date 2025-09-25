{% macro sk_hash(cols) -%}
  {{ dbt_utils.generate_surrogate_key(cols) }}
{%- endmacro %}
