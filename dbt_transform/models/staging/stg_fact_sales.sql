{{
    config(
        materialized = 'table',
        schema = 'staging',
    )
}}

with cte_source as (
SELECT 
  *
  except(_id,
    user_agent,
    option,
    option_list,
    option_object,
    cart_products),
  cart_products_unnest.product_id as cart_product_id,
  cart_products_unnest.amount as cart_amount,
  cart_products_unnest.price as cart_price,
  cart_products_unnest.currency as cart_currency
FROM {{ source('glamira_sources', 'summary_raw') }}
CROSS JOIN
  UNNEST(cart_products) as cart_products_unnest
where collection = "checkout_success"
)
, cte_filter_sales_columns as (
  select
    cast(trim(order_id) as string) as order_id,
    timestamp_seconds(cast(time_stamp as int64)) as utc_timestamp,
    cast(trim(local_time) as datetime) as local_datetime,
    cast(trim(store_id) as string) as store_id,
    cast(trim(ip) as string) as ip_address,
    cast(trim(user_id_db) as string) as user_id_db,
    cast(trim(email_address) as string) as email_address,
    cast(trim(cart_product_id) as string) as product_id,
    cast(trim(cart_currency) as string) as currency,
    cast(cart_amount as int64) as product_qty,
    cast(trim(cart_price) as string) as unit_price
  from cte_source
)
, cte_format_data as (
  select
    cast(regexp_extract(order_id,'^([0-9]+)[.]*') as string) as order_id,
    utc_timestamp,
    local_datetime,
    cast(utc_timestamp as date) as utc_order_date,
    cast(local_datetime as date) as local_order_date,
    cast(format_timestamp('%Y%m%d', utc_timestamp) as int64) as order_date_key,
    nullif(store_id,'') as store_id,
    nullif(ip_address,'') as ip_address,
    nullif(user_id_db,'') as user_id_db,
    nullif(email_address,'') as email_address,
    nullif(product_id,'') as product_id,
    nullif(currency,'') as currency,
    product_qty,
    cast(
      CASE 
          -- Nếu số có dạng số.số,số (Kiểu Châu Âu: có dấu chấm ở trước dấu phẩy)
        WHEN REGEXP_CONTAINS(unit_price, r'\..*,')
          THEN REGEXP_REPLACE(REGEXP_REPLACE(unit_price, r'\.', ''), r',', '.')
        
        -- Nếu số có dạng số,số.số (Kiểu Mỹ: có dấu phẩy ở trước dấu chấm)
        WHEN REGEXP_CONTAINS(unit_price, r',.*\.')
          THEN REGEXP_REPLACE(unit_price, r',', '')
        
        -- Nếu số có dạng số'số.số (Kiểu có dấu nháy ở trước dấu chấm)
        WHEN REGEXP_CONTAINS(unit_price, r"\'.*\.")
          THEN REGEXP_REPLACE(unit_price, r"\'", '')
        
      -- Nếu số kết thúc với ,00 hoặc ٫00 (Kiểu có dấu phẩy làm thập phân)
      WHEN REGEXP_CONTAINS(unit_price, r'[^0-9]00$')
        THEN REGEXP_REPLACE(unit_price, r'[^0-9]00$', '')
      
      -- Nếu trống -> null
      WHEN unit_price = '' THEN null
      -- Các trường hợp còn lại nếu có dấu phẩy thừa thì xóa nốt
      ELSE REGEXP_REPLACE(unit_price, r',', '')
      END
    as numeric) AS unit_price,
    row_number() over(order by utc_timestamp) as row_num
  {# from {{ ref('stg_fact_sales') }} #}
  from cte_filter_sales_columns
  
)


select
*
{# from cte_filter_sales_columns #}
from cte_format_data