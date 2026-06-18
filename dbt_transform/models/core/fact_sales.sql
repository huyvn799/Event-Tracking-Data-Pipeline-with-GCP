{{
    config(
        materialized = 'incremental',
        schema = 'core',
        partition_by = {
          "field": "utc_order_date",
          "data_type": "date"
        },
        cluster_by = ['from_currency_key'],
        unique_key = ['exchange_rate_key'],
        incremental_strategy = 'merge',
        merge_update_columns = ['exchange_rate', 'updated_at', 'updated_by']
    )
}}

with cte_source as (
SELECT 
  *
  except(_id,
    user_agent,
    option,
    option_list,
    option_object
    cart_products),
  cart_products_unnest.product_id as cart_product_id,
  cart_products_unnest.amount as cart_amount,
  cart_products_unnest.price as cart_price,
  cart_products_unnest.currency as cart_currency
  FROM `glamira-dec-k23-huy.bronze.summary_raw`
CROSS JOIN
  UNNEST(cart_products) as cart_products_unnest
where collection = "checkout_success"
)
, cte_filter_sales_columns as (
  select
    timestamp_seconds(cast(timestamp as int64)) as utc_timestamp,
    trim(ip) as ip_address,
    trim(user_id_db) as user_id_db,
    trim(store_id) as store_id,
    cast(local_time as datetime) as local_datetime,
    trim(email_address) as email_address,
    cast(regexp_extract(order_id,'^([0-9]+)\.0$')) as order_id,

  from cte_source
)
