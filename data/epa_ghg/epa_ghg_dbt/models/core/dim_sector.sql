{{ config(materialized='table', schema='STG_CORE') }}

select
  md5(coalesce(industry_sector, '')) as sector_sk,
  industry_sector as sector
from (
  select distinct industry_sector
  from {{ ref('stg_ghg') }}
)
