{{
  config(
    materialized = 'view',
    schema = 'staging'
    )
}}
with date_series as (
    select full_date
    from unnest(
        GENERATE_DATE_ARRAY('2018-01-01', CURRENT_DATE(), INTERVAL 1 DAY)
    ) as full_date
)

select
  CAST(FORMAT_DATE('%Y%m%d', full_date) AS INT64) as date_key,
  full_date,
  CAST(EXTRACT(YEAR FROM full_date) as INT64) as year_num,
  CAST(EXTRACT(QUARTER FROM full_date) as INT64) as quarter_num,
  CAST(CONCAT('Quarter ', EXTRACT(QUARTER FROM full_date)) as STRING) as quarter_name,
  CAST(EXTRACT(MONTH FROM full_date) as INT64) as month_num,
  CAST(FORMAT_DATE('%B', full_date) as STRING) as full_month_name,
  CAST(FORMAT_DATE('%Y-%m', full_date) as STRING) as year_month,
  CAST(EXTRACT(DAY FROM full_date) as INT64) as day_of_month,
  CAST(EXTRACT(DAYOFWEEK FROM full_date) as INT64) as day_of_week,
  CAST(FORMAT_DATE('%A', full_date) as STRING) as full_day_name,
  IF(EXTRACT(DAYOFWEEK FROM full_date) IN (1, 7), TRUE, FALSE) AS is_weekend

from date_series