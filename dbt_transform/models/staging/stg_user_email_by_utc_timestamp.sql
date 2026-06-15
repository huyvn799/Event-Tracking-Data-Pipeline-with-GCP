{{
    config(
        materialized = 'view',
        schema = 'staging'
    )
}}

with cte_user_email_from_raw as (
  SELECT  
    trim(user_id_db) as user_id_db,
    nullif(trim(email_address),'') as email_address,
    cast(trim(local_time) as datetime) as local_datetime,
    timestamp_seconds(time_stamp) as utc_timestamp
  FROM {{ source('glamira_sources', 'summary_raw') }}
  where user_id_db is not null and trim(user_id_db) <> ''
  -- order by user_id_db, local_datetime
)
, cte_detect_changes as (
  select 
    user_id_db,
    email_address,
    local_datetime,
    utc_timestamp,
    case
      when coalesce(lag(email_address) over(
        partition by user_id_db 
        order by utc_timestamp asc),'') = coalesce(email_address,'')
      then 0 else 1 
    end as is_new_state
  from cte_user_email_from_raw
)
, cte_group_user_email as (
  select 
    user_id_db,
    email_address,
    local_datetime,
    utc_timestamp,
    SUM(is_new_state) OVER (
      PARTITION BY user_id_db 
      ORDER BY utc_timestamp) as group_user_email
  from cte_detect_changes
)
, cte_find_earliest_boundary_by_timestamp as (
  select
    user_id_db,
    email_address,
    group_user_email,
    min(local_datetime) as start_local_datetime,
    min(utc_timestamp) as start_utc_timestamp
  from cte_group_user_email
  group by user_id_db, email_address, group_user_email
)

  select 
  *
  from cte_find_earliest_boundary_by_timestamp
  -- where email_address is null