{{
    config(
        materialized = 'incremental',
        schema = 'core',
        cluster_by = ['date_key', 'full_date'],
        unique_key = ['date_key'],
        incremental_strategy = 'merge'
    )
}}

select 
    date_key,
    full_date,
    year_num,
    quarter_num,
    quarter_name,
    month_num,
    full_month_name,
    year_month,
    day_of_month,
    day_of_week,
    full_day_name,
    is_weekend
from {{ ref('stg_generate_date') }}
{% if is_incremental() %}
where full_date > (select max(full_date) from {{ this }})
{% endif %}