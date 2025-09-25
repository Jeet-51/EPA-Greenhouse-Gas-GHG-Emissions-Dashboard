select 
    f.reporting_year,
    s.state_code,
    s.state_name,
    f.sector,
    f.emissions
from {{ ref('fct_emissions') }} f
join {{ ref('dim_state') }} s
  on f.state_sk = s.state_sk
