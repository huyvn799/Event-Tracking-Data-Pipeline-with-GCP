{{ config(
    materialization = 'incremental',
    cluster_by = ['user_id_db', 'start_utc_timestamp'],
    unique_key = ['scd_customer_key'],
    incremental_strategy = 'merge',
    merge_update_columns = [
      'email_address',
      'end_utc_date',
      'updated_at',
      'updated_by'
    ]
) }}

with cte_convert_to_date as (
select 
  user_id_db,
  email_address,
  cast(start_local_datetime as date) as start_local_date,
  cast(start_utc_timestamp as date) as start_utc_date,
  start_local_datetime,
  start_utc_timestamp,
  LEAD(start_utc_timestamp) OVER (
      PARTITION BY user_id_db
      ORDER BY start_utc_timestamp ASC
    ) as end_utc_timestamp,
from {{ ref('stg_user_email_by_utc_timestamp') }}
)

, cte_get_end_date as (
  select
    user_id_db,
    email_address,
    start_local_date,
    start_utc_date,
    start_local_datetime,
    start_utc_timestamp,
    end_utc_timestamp,
    cast(end_utc_timestamp as date) as end_utc_date
  from cte_convert_to_date
)
, cte_is_current as (
  select
    farm_fingerprint(concat(user_id_db, cast(start_utc_timestamp as string))) as scd_customer_key,
    user_id_db,
    coalesce(email_address, 'Unknown') as email_address,
    start_local_date,
    start_local_datetime,
    start_utc_date,
    start_utc_timestamp,
    coalesce(end_utc_date, DATE(9999,12,31)) as end_utc_date,
    coalesce(end_utc_timestamp, TIMESTAMP(DATE(9999,12,31))) as end_utc_timestamp,
    case
      when end_utc_timestamp is null then true
      else false
    end as is_current,
    case
      when email_address is null then false
      else true
    end as has_email_info,
  from cte_get_end_date
)
, cte_final as (
  select
  cast(scd_customer_key as int64) as scd_customer_key,
  cast(user_id_db as string) as user_id_db,
  cast(email_address as string) as email_address,
  cast(start_utc_date as date) as start_utc_date,
  cast(end_utc_date as date) as end_utc_date,
  cast(start_utc_timestamp as timestamp) as start_utc_timestamp,
  cast(end_utc_timestamp as timestamp) as end_utc_timestamp,
  cast(is_current as boolean) as is_current,
  cast(has_email_info as boolean) as has_email_info
from cte_is_current
)

select
  stg.scd_customer_key,
  stg.user_id_db,
  stg.email_address,
  stg.start_utc_date,
  stg.end_utc_date,
  stg.start_utc_timestamp,
  stg.end_utc_timestamp,
  stg.is_current,
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
    on stg.scd_customer_key = old.scd_customer_key
{% else %}

union all
  select
  -1 as scd_customer_key,
  '-1' as user_id_db,
  'Unknown' as email_address,
  DATE(1900,1,1) as start_utc_date,
  DATE(9999,12,31) as end_utc_date,
  TIMESTAMP(DATE(1900,1,1)) as start_utc_timestamp,
  TIMESTAMP(DATE(9999,12,31)) as end_utc_timestamp,
  false as is_current,
  false as has_email_info,
  {{ generate_created_columns() }},
  {{ generate_updated_columns() }}
  
{% endif %}