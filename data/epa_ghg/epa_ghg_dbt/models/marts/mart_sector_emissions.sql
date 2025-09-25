{{ config(schema='MART', materialized='view') }}

WITH base AS (
  SELECT s.sector, fe.reporting_year, SUM(fe.emissions_mtco2e) AS emissions
  FROM {{ ref('fct_emissions') }} fe
  JOIN {{ ref('dim_sector') }} s USING (sector_sk)
  GROUP BY 1,2
)
SELECT
  *,
  DENSE_RANK() OVER (PARTITION BY reporting_year ORDER BY emissions DESC) AS rank_in_year
FROM base
