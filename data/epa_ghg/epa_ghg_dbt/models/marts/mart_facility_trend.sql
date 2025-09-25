-- models/marts/mart_facility_trend.sql
{{ config(materialized='view') }}

with base as (
  select
    fe.facility_sk,
    fe.state_sk,
    fe.reporting_year,
    sum(fe.emissions_mtco2e) as emissions
  from {{ ref('fct_emissions') }} fe
  group by 1,2,3
)

select
  df.facility_name,
  ds.state,
  b.reporting_year,
  b.emissions
from base b
join {{ ref('dim_facility') }} df
  on df.facility_sk = b.facility_sk
join {{ ref('dim_state') }} ds
  on ds.state_sk = b.state_sk
