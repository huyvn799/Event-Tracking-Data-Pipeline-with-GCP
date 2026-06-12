{{
    config(
        materialized='incremental',
        schema='core',
        unique_key = ['date_key', 'from_currency_key', 'to_currency_key'],
        incremental_strategy = 'merge',
        merge_update_columns = ['exchange_rate', 'updated_at', 'updated_by']
    )
}}

with cte_currencies_with_raw_symbol_csv as (
    select 
        cast(currency_code as string) as currency_code,
        cast(raw_symbol as string)as raw_currency_symbol
    from {{ ref('currencies_with_raw_symbol') }}
)

, cte_dim_currency as (
    select 
        cast(currency_key as int64) as currency_key,
        cast(currency_code as string) as currency_code,
        cast(currency_name as string) as currency_name
    from {{ ref('dim_currency') }}
)

, cte_exchange_rates_2019_2020_csv as (
    select 
        cast(date_key as int64) as date_key,
        cast(from_currency_code as string) as from_currency_code,
        cast(to_currency_code as string) as to_currency_code,
        cast(exchange_rate as numeric) as exchange_rate
    from {{ ref('exchange_rates_2019_2020') }}
)

, cte_fact_snapshot_exchange_rate as (
    select
        d.date_key,
        c.from_currency_code,
        c.to_currency_code,
    from {{ ref('dim_date') }} d
    left join cte_exchange_rates_2019_2020_csv r
        on d.date_key = r.date_key
            and d_currency.currency_code = r.from_currency_code
    cross join cte_dim_currency c
)

, cte_forward_fill_for_fact as (
    select
        *
    from cte_fact_snapshot_exchange_rate
)
