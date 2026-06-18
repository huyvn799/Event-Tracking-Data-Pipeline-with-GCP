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

, cte_forward_fill as (
select
 user_id_db,
 email_address as email_address_have_null,
 local_datetime,
 utc_timestamp,
 LAST_VALUE(email_address IGNORE NULLS) OVER (
  PARTITION BY user_id_db
  ORDER BY utc_timestamp ASC
  ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
 ) as email_address,
 MIN(CASE
  WHEN email_address IS NOT NULL THEN utc_timestamp END) OVER (
  PARTITION BY user_id_db
 ) as first_email_timestamp
from cte_user_email_from_raw
)
, cte_label_leading_nulls AS (
  select
      user_id_db,
    CASE 
        -- Nếu dòng này là null và xảy ra TRƯỚC KHI có email đầu tiên -> Unspecified
        WHEN email_address IS NULL AND (utc_timestamp < first_email_timestamp OR first_email_timestamp IS NULL) 
            THEN 'Unspecified'
        ELSE email_address
    END AS email_labeled,
    local_datetime,
    utc_timestamp,
  from cte_forward_fill
)
, cte_detect_changes as (
  select 
    user_id_db,
    email_labeled,
    local_datetime,
    utc_timestamp,
    LAG(email_labeled) over(
        partition by user_id_db
        order by utc_timestamp asc
    ) as prev_email
  from cte_label_leading_nulls
)

, cte_mark_changes as (
  select
    user_id_db,
    email_labeled as email_address,
    prev_email,
    local_datetime,
    utc_timestamp,
    case
      when email_labeled <> prev_email then 1
      else 0
    end as is_new_state
  from cte_detect_changes
)
, cte_group_user_email as (
  select 
    user_id_db,
    nullif(email_address,'Unspecified') as email_address,
    local_datetime,
    utc_timestamp,
    SUM(is_new_state) OVER (
      PARTITION BY user_id_db 
      ORDER BY utc_timestamp) as group_user_email
  from cte_mark_changes
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