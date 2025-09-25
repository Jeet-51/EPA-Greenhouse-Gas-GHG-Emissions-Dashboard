{{ config(schema='MART', materialized='view') }}

WITH base AS (
  SELECT ds.state, ds.region, fe.reporting_year, SUM(fe.emissions_mtco2e) AS emissions
  FROM {{ ref('fct_emissions') }} fe
  JOIN {{ ref('dim_state') }} ds USING (state_sk)
  GROUP BY 1,2,3
)
SELECT
  *,
  LAG(emissions) OVER (PARTITION BY state ORDER BY reporting_year) AS emissions_prior,
  CASE WHEN LAG(emissions) OVER (PARTITION BY state ORDER BY reporting_year) IS NULL
       OR LAG(emissions) OVER (PARTITION BY state ORDER BY reporting_year)=0
       THEN NULL
       ELSE (emissions - LAG(emissions) OVER (PARTITION BY state ORDER BY reporting_year))
            / NULLIF(LAG(emissions) OVER (PARTITION BY state ORDER BY reporting_year),0)
  END AS yoy_pct
FROM base
