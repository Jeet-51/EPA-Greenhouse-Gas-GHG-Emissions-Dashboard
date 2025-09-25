{{ config(schema='CORE', materialized='table') }}

WITH base AS (
  SELECT DISTINCT facility_name, state, county FROM {{ ref('stg_ghg') }}
),
dstate AS (SELECT state_sk, state FROM {{ ref('dim_state') }})
SELECT
  {{ sk_hash(['b.facility_name','b.state','b.county']) }} AS facility_sk,
  b.facility_name,
  b.county,
  dstate.state_sk
FROM base b
LEFT JOIN dstate ON dstate.state = b.state
