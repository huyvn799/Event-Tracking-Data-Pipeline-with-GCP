{{
  config(
    materialized = 'view',
    schema = 'staging'
    )
}}

with cte_process_quote as (
  SELECT     
    trim(ip_address) as ip_address,
    case when country_name = 'Namibia' 
            then coalesce(country_code, 'NA') 
            else country_code 
    end as country_code,
    trim(country_name,"'") as country_name,
    trim(region,"'") as region,
    trim(city,"'") as city
  FROM {{ source('glamira_sources', 'ip_locations') }}
)
, cte_process_null as (
select 
ip_address,
  coalesce(nullif(country_code,'-'), 'Unknown') as country_code,
  coalesce(nullif(country_name,'-'), 'Unknown') as country_name,
  coalesce(nullif(region,'-'), 'Unknown') as region,
  coalesce(nullif(city,'-'), 'Unknown') as city
from cte_process_quote
)
, cte_generate_location_key as (
select
  ip_address,
  farm_fingerprint(concat(country_code, region, city)) as location_key,
  country_code,
  country_name,
  region,
  city
from cte_process_null
where ip_address <> 'unknown'
)

select
  ip_address,
  case when
    country_code = 'Unknown'
    and region = 'Unknown'
    and city = 'Unknown' then -1 else location_key 
  end as location_key,
  country_code,
  country_name,
  region,
  city
from cte_generate_location_key