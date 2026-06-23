{{
    config(
        materialized = 'table',
        schema = 'core',
        cluster_by = ['location_key', 'country_code'],
    )
}}

select 
    distinct
    cast(location_key as int64) as location_key,
    cast(country_code as string) as country_code,
    cast(country_name as string) as country_name,
    cast(region as string) as region,
    cast(city as string) as city,
    {{ generate_created_columns() }},
    {{ generate_updated_columns() }}
from {{ ref('stg_ip_location_mapping') }}