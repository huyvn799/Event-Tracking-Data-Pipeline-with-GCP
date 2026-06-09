{{
    config(
        materialized='incremental',
        unique_key='date_key',
        incremental_strategy='merge'
    )
}}

select 
    *
from {{ ref('stg_generate_date') }}
{% if is_incremental() %}
where full_date > (select max(full_date) from {{ this }})
{% endif %}