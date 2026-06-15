{{
    config(
        materialized = 'incremental',
        schema = 'core',
        partition_by = {
          "field": "full_date",
          "data_type": "date"
        },
        cluster_by = ['from_currency_key'],
        unique_key = ['exchange_rate_key'],
        incremental_strategy = 'merge',
        merge_update_columns = ['exchange_rate', 'updated_at', 'updated_by']
    )
}}

with min_date as (
  select min(date_key) as min_date from {{ ref('exchange_rates_2019_2020') }}
)
, max_date as (
  select max(date_key) as max_date from {{ ref('exchange_rates_2019_2020') }}
)
, cte_filter_min_date as (
  select
  distinct
  d.date_key
  from `glamira_core.dim_date` d
  cross join min_date m
  where d.date_key >= m.min_date
)
, cte_filter_min_and_max_date as (
  select
  distinct
  d.date_key
  from cte_filter_min_date d
  cross join max_date m
  where d.date_key <= m.max_date
)
, cte_join_currency_from_to as (
  select
  d.date_key,
  from_c.currency_code as from_currency_code,
  from_c.currency_key as from_currency_key,
  to_c.to_currency_code as to_currency_code,
  to_c.to_currency_key as to_currency_key
  from cte_filter_min_and_max_date d
  cross join {{ ref('dim_currency') }} from_c
  cross join (
    select 
      currency_code as to_currency_code, 
      currency_key as to_currency_key 
    from {{ ref('dim_currency') }}
    where currency_code in ('USD')) to_c
  where from_c.currency_key <> -1
)
, cte_join_rate as (
  select
  t.date_key,
  t.from_currency_code,
  t.from_currency_key,
  t.to_currency_code,
  t.to_currency_key,
  r.exchange_rate
  from cte_join_currency_from_to t
  left join `glamira_staging.stg_exchange_rates_csv` r
  on t.date_key = r.date_key
    and t.from_currency_code = r.from_currency_code
)
, cte_forward_fill_for_rate_null as (
  select
  t.date_key,
  t.from_currency_code,
  t.from_currency_key,
  t.to_currency_code,
  t.to_currency_key,
  LAST_VALUE(t.exchange_rate IGNORE NULLS) OVER (
    PARTITION BY t.from_currency_code, t.to_currency_code
    ORDER BY t.date_key
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) as exchange_rate
  from cte_join_rate t
)
, cte_final as (
    select
        farm_fingerprint(concat(cast(t.date_key as string), t.from_currency_code, t.to_currency_code)) as exchange_rate_key,
        t.date_key,
        format_date('%Y-%m-%d',parse_date('%Y%m%d', cast(date_key as string))) as full_date,
        t.from_currency_code,
        t.from_currency_key,
        t.to_currency_code,
        t.to_currency_key,
        t.exchange_rate
    from cte_forward_fill_for_rate_null t
    where t.exchange_rate is not null
)
{% if is_incremental() %}
    select
    old.exchange_rate_key,
    old.date_key,
    old.full_date,
    old.from_currency_key,
    old.to_currency_key,
    t.exchange_rate,
    old.created_at,
    old.created_by,
    current_timestamp() as updated_at,
    session_user() as updated_by
    from cte_final t
    inner join {{ this }} as old 
        on t.exchange_rate_key = old.exchange_rate_key 
    where t.exchange_rate <> old.exchange_rate

  union all
    select
    farm_fingerprint(concat(cast(t.date_key as string), t.from_currency_code, t.to_currency_code)) as exchange_rate_key,
    t.date_key,
    t.full_date,
    t.from_currency_key,
    t.to_currency_key,
    t.exchange_rate,
    {{ generate_created_columns() }},
    {{ generate_updated_columns() }}
    from cte_final t
    left join {{ this }} as old 
        on t.exchange_rate_key = old.exchange_rate_key 
    where old.exchange_rate_key is null
{% else %}
    select
    t.exchange_rate_key,
    t.date_key,
    t.full_date,
    t.from_currency_key,
    t.to_currency_key,
    t.exchange_rate,
    {{ generate_created_columns() }},
    {{ generate_updated_columns() }}
    from cte_final t
{% endif %}
