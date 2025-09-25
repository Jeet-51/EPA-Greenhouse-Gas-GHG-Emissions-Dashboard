-- Create database & compute
CREATE DATABASE IF NOT EXISTS EPA_GHG;
CREATE WAREHOUSE IF NOT EXISTS WH_GHG
  WITH WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND = 120
  AUTO_RESUME = TRUE
  INITIALLY_SUSPENDED = TRUE;

USE DATABASE EPA_GHG;
USE WAREHOUSE WH_GHG;

-- Schemas for the lifecycle
CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STG;
CREATE SCHEMA IF NOT EXISTS CORE;
CREATE SCHEMA IF NOT EXISTS MART;

-- One Unified RAW Table
CREATE OR REPLACE TABLE RAW.RAW_GHG_EMISSIONS (
  facility_name STRING,
  state STRING,
  county STRING,
  industry_sector STRING,
  year INTEGER,
  reported_emissions_mtco2e NUMBER(38,2),
  _load_filename STRING,
  _load_ts TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP
);

SELECT
  CURRENT_ACCOUNT()         AS account_locator,
  CURRENT_REGION()          AS region,
  CURRENT_ORGANIZATION_NAME() AS org_name,
  CURRENT_ACCOUNT_NAME()    AS account_name;



use role ACCOUNTADMIN;            -- or your build role

use warehouse WH_GHG;             -- your warehouse
use database EPA_GHG;             -- your database
-- list schemas to verify they exist
show schemas in database EPA_GHG;



use schema EPA_GHG.STG_CORE;
show objects in schema EPA_GHG.STG_CORE;

select * from STG.DIM_STATE limit 5;

select * from STG_MART.mart_state_emissions;

select distinct state from STG.dim_state order by state;

select count(*) as total_rows
from EPA_GHG.RAW.RAW_GHG_EMISSIONS;

select reporting_year, count(*) as facilities
from EPA_GHG.STG.STG_GHG
group by reporting_year
order by reporting_year;

-- state
select reporting_year,
       state,
       sum(emissions) as total_emissions
from EPA_GHG.STG_MART.MART_STATE_EMISSIONS
group by reporting_year, state
order by reporting_year, total_emissions desc;

-- sector 
select reporting_year, sector, emissions, rank_in_year
from EPA_GHG.STG_MART.MART_SECTOR_EMISSIONS
order by reporting_year, rank_in_year
limit 15;

-- facility
select facility_name, state, reporting_year, emissions
from EPA_GHG.STG.MART_FACILITY_TREND
where facility_name ilike '%POWER%'
order by facility_name, reporting_year;


select distinct sector
from EPA_GHG.STG.DIM_SECTOR
order by 1;

select state, sector, reporting_year, sum(emissions_mtco2e) as emissions
from EPA_GHG.STG.FCT_EMISSIONS f
join EPA_GHG.STG.DIM_SECTOR s on f.sector_sk = s.sector_sk
join EPA_GHG.STG.DIM_STATE st on f.state_sk = st.state_sk
where reporting_year = 2021
group by 1,2,3
order by emissions desc;





