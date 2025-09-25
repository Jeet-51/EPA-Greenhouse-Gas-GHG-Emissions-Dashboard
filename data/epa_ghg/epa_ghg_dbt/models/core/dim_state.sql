{{ config(materialized='table', schema='STG_CORE') }}

with s as (
  select distinct state
  from {{ ref('stg_ghg') }}
),
regions as (
  select upper(state) as state, region
  from {{ ref('us_state_regions') }}
)

select
  md5(coalesce(s.state, '')) as state_sk,
  s.state,
  r.region
from s
left join regions r using (state)
