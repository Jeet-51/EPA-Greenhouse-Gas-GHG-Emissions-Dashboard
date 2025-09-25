{{ config(schema='MART', materialized='view') }}

SELECT 'STATE' AS grain_type, state AS grain_name, CAST(NULL AS STRING) AS grain_extra,
       reporting_year, emissions, emissions_prior, yoy_pct, NULL AS rank_in_year
FROM {{ ref('mart_state_emissions') }}
UNION ALL
SELECT 'SECTOR', sector, NULL, reporting_year, emissions, NULL, NULL, rank_in_year
FROM {{ ref('mart_sector_emissions') }}
UNION ALL
SELECT 'FACILITY', facility_name, state, reporting_year, emissions, NULL, NULL, NULL
FROM {{ ref('mart_facility_trend') }}
