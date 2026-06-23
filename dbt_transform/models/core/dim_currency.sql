{{
    config(
        materialized = 'table',
        schema = 'core',
        cluster_by = ['currency_key', 'currency_code'],
    )
}}

select 
    farm_fingerprint(cast(currency_code as string)) as currency_key,
    cast(currency_code as string) as currency_code,
    cast(currency_name as string) as currency_name,
    {{ generate_created_columns() }},
    {{ generate_updated_columns() }}
from {{ ref('currencies_with_raw_symbol') }}
union all
select 
    -1 as currency_key,
    'Unknown' as currency_code,
    'Unknown' as currency_name,
    {{ generate_created_columns() }},
    {{ generate_updated_columns() }}