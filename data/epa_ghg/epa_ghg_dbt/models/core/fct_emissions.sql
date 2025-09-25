WITH g AS (SELECT * FROM {{ ref('stg_ghg') }}),
dstate AS (SELECT state_sk, state FROM {{ ref('dim_state') }}),
g_plus AS (
  SELECT g.*, dstate.state_sk
  FROM g LEFT JOIN dstate ON dstate.state = g.state
),
dsect  AS (SELECT sector_sk, sector FROM {{ ref('dim_sector') }}),
dfac   AS (SELECT facility_sk, facility_name, state_sk FROM {{ ref('dim_facility') }})

SELECT
  dfac.facility_sk,
  dsect.sector_sk,
  g_plus.state_sk,
  g_plus.reporting_year,
  g_plus.emissions_mtco2e
FROM g_plus
LEFT JOIN dfac  ON dfac.facility_name = g_plus.facility_name AND dfac.state_sk = g_plus.state_sk
LEFT JOIN dsect ON dsect.sector       = g_plus.industry_sector
