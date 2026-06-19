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

with cte_format_data as (
  select
    cast(regexp_extract(order_id,'^([0-9]+)[.]*') as string) as order_id,
    utc_timestamp,
    local_datetime,
    store_id,
    ip_address,
    user_id_db,
    email_address,
    product_id,
    currency,
    product_qty,
    CASE 
      -- Nếu số có dạng chữ.chữ,chữ (Kiểu Châu Âu: có dấu chấm ở trước dấu phẩy)
      WHEN REGEXP_CONTAINS(unit_price, r'\..*,') 
        THEN REGEXP_REPLACE(REGEXP_REPLACE(unit_price, r'\.', ''), r',', '.')
      
      -- Nếu số có dạng chữ,chữ.chữ (Kiểu Mỹ: có dấu phẩy ở trước dấu chấm)
      WHEN REGEXP_CONTAINS(unit_price, r',.*\.') 
        THEN REGEXP_REPLACE(unit_price, r',', '')
      
      -- Nếu số có dạng chữ'chữ.chữ (Kiểu có dấu nháy ở trước dấu chấm)
      WHEN REGEXP_CONTAINS(unit_price, r'\..*,') 
        THEN REGEXP_REPLACE(REGEXP_REPLACE(unit_price, r'\.', ''), r',', '.')

      -- Các trường hợp còn lại nếu có dấu phẩy thừa thì xóa nốt
      ELSE REGEXP_REPLACE(unit_price, r',', '')
    END 
  AS unit_price

  from {{ ref('stg_fact_sales') }}
  
)


select
*
from cte_format_data
