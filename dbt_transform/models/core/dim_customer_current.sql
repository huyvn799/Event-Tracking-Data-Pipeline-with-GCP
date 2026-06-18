{{ config(
    materialization = 'incremental',
    cluster_by = ['user_id_db', 'active_date'],
    unique_key = ['customer_key'],
    incremental_strategy = 'merge',
    merge_update_columns = [
      'email_address',
      'active_date'
      'updated_at',
      'updated_by'
    ]

) }}

with cte_forward_fill as (
select
 user_id_db,
 email_address as email_address_have_null,
 start_local_datetime,
 start_utc_timestamp,
 LAST_VALUE(email_address IGNORE NULLS) OVER (
  PARTITION BY user_id_db
  ORDER BY start_utc_timestamp ASC
 ) as email_address
from {{ ref('stg_user_email_by_utc_timestamp') }}
)
, cte_convert_to_date as (
select 
  user_id_db,
  email_address,
  start_local_datetime,
  start_utc_timestamp,
  cast(start_local_datetime as date) as start_local_date,
  cast(start_utc_timestamp as date) as start_utc_date
from cte_forward_fill
)
, cte_get_latest_email as (
  select
  user_id_db,
  start_utc_date,
  start_utc_timestamp,
  email_address,
  LAST_VALUE(email_address) OVER (
    PARTITION BY user_id_db, start_utc_date
    ORDER BY start_utc_timestamp ASC
  ) as latest_email_address,
  FIRST_VALUE(start_utc_date) OVER (
    PARTITION BY user_id_db
    ORDER BY start_utc_timestamp ASC
  ) as first_start_date
  
  from cte_convert_to_date
)
, cte_last_email_and_first_start_date as (
  select
    distinct
    user_id_db,
    latest_email_address as email_address,
    first_start_date as first_start_date
  from cte_get_latest_email
)

, cte_generate_key as (
  select
    farm_fingerprint(user_id_db) as customer_key,
    user_id_db,
    coalesce(email_address, 'Unknown') as email_address,
    first_start_date as active_date,
    case
      when email_address is null then false
      else true
    end as has_email_info,
  from cte_last_email_and_first_start_date
)
, cte_final as (
  select
  cast(customer_key as int64) as customer_key,
  cast(user_id_db as string) as user_id_db,
  cast(email_address as string) as email_address,
  cast(active_date as date) as active_date,
  cast(has_email_info as boolean) as has_email_info
from cte_generate_key
)

select
  stg.customer_key,
  stg.user_id_db,
  stg.email_address,
  stg.active_date,
  stg.has_email_info,
  {% if is_incremental() %}
    coalesce(old.created_at, current_timestamp()) as created_at,
    coalesce(old.created_by, session_user()) as created_by,
    current_timestamp() as updated_at,
    session_user() as updated_by
  {% else %}
    {{ generate_created_columns() }},
    {{ generate_updated_columns() }}
  {% endif %}
from cte_final stg
{% if is_incremental() %}
  left join {{ this }} old
    on stg.customer_key = old.customer_key
{% else %}

union all
  select
  -1 as customer_key,
  '-1' as user_id_db,
  'Unknown' as email_address,
  DATE(1900,1,1) as active_date,
  false as has_email_info,
  {{ generate_created_columns() }},
  {{ generate_updated_columns() }}
  
{% endif %}
