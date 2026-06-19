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

select
*
from cte_filter_sales_columns
