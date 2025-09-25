{{ config(materialized='view') }}

with src as (
  select
    trim(facility_name)                                 as facility_name,
    upper(trim(state))                                  as state,
    replace(upper(trim(county_name)), ' COUNTY', '')    as county,
    cast(reporting_year as integer)                     as reporting_year,
    try_to_decimal(ghg_quantity_mtco2e, 38, 2)          as emissions_mtco2e,
    subparts                                            as subparts_raw
  from {{ source('raw', 'raw_ghg_emissions') }}
  where reporting_year between 2021 and 2023
    and facility_name is not null
    and state is not null
),
map_candidates as (
  select distinct subpart, sector
  from {{ ref('subpart_to_sector_map') }}
),
mapped as (
  select
    s.*,
    coalesce(mc.sector, 'Other') as industry_sector
  from src s
  left join map_candidates mc
    on s.subparts_raw ilike '%' || mc.subpart || '%'
)
select
  facility_name,
  state,
  county,
  industry_sector,
  reporting_year,
  emissions_mtco2e
from mapped
