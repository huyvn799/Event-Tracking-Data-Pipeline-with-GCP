{{ config(
    materialization = 'table',
    unique_key = ['customer_key']
    cluster_by = ['user_id_db', 'start_utc_date']
) }}

with cte_forward_fill as (
select
 user_id_db,
 email_address as email_address_null,
 start_local_datetime,
 start_utc_timestamp,
 LAST_VALUE(email_address IGNORE NULLS) OVER (
  PARTITION BY user_id_db
  ORDER BY start_utc_timestamp ASC
 ) as email_address
from {{ ref('stg_user_email_by_utc_timestamp') }}
)
,cte_convert_to_date as (
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
  FIRST_VALUE(email_address) OVER (
    PARTITION BY user_id_db, start_utc_date
    ORDER BY start_utc_timestamp DESC
  ) as latest_email_address
  
  from cte_convert_to_date
)
, cte_group_user_email_date as (
  select
    user_id_db,
    start_utc_date,
    latest_email_address as email_address,
    max(start_utc_timestamp) as latest_start_utc_timestamp
  from cte_get_latest_email
  group by user_id_db, start_utc_date, latest_email_address
)
, cte_get_end_date as (
  select
    user_id_db,
    email_address,
    start_utc_date,
    LEAD(start_utc_date) OVER (
      PARTITION BY user_id_db
      ORDER BY start_utc_date ASC
    ) as end_utc_date
  from cte_group_user_email_date
)
,cte_is_current as (
  select
    farm_fingerprint(concat(user_id_db, start_utc_date)) as customer_key,
    user_id_db,
    email_address,
    start_utc_date,
    coalesce(end_utc_date, DATE(9999,12,31)) as end_utc_date,
    case
      when end_utc_date is null then true
      else false
    end as is_current,
    true as has_db_info
  from cte_get_end_date
)

  
select
  customer_key,
  user_id_db,
  email_address,
  start_utc_date, 
  end_utc_date,
  is_current,
  has_db_info,
  {{ generate_created_columns() }},
    {{ generate_updated_columns() }}
from cte_is_current
union all
  select
  -1 as customer_key,
  '-1' as user_id_db,
  'Unknown' as email_address,
  DATE(1900,1,1) as start_utc_date, 
  DATE(9999,12,31) as end_utc_date,
  false as is_current,
  false as has_db_info,
  {{ generate_created_columns() }},
    {{ generate_updated_columns() }}
