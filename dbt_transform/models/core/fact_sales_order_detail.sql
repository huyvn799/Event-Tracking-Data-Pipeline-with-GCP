{{
    config(
        materialized = 'incremental',
        schema = 'core',
        partition_by = {
          "field": "local_order_date",
          "data_type": "date"
        },
        cluster_by = ['order_date_key', 'order_id', 'scd_customer_key'],
        unique_key = ['sales_key'],
        incremental_strategy = 'merge',
        merge_update_columns = [
          'local_unit_price',
          'usd_unit_price',
          'local_amount',
          'usd_amount',
          'updated_at',
          'updated_by'
        ]
    )
}}
{# {{
    config(
        materialized = 'table',
        schema = 'core',
    )
}} #}

with cte_format_data as (
  {# select
    cast(regexp_extract(order_id,'^([0-9]+)[.]*') as string) as order_id,
    utc_timestamp,
    local_datetime,
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
    as numeric) AS unit_price #}

    select
    order_id,
    utc_timestamp,
    local_datetime,
    utc_order_date,
    local_order_date,
    order_date_key,
    store_id,
    ip_address,
    user_id_db,
    email_address,
    product_id,
    currency,
    product_qty,
    unit_price,

  from {{ ref('stg_fact_sales') }}
  
)

, cte_raw_symbol as (
  select
  distinct
  raw_symbol,
  currency_code
  from {{ ref('currencies_with_raw_symbol') }}
  where raw_symbol is not null
)
, cte_sales_join_raw_symbol as (
  SELECT 
  f.order_id,
  f.utc_timestamp,
  f.local_datetime,
  f.utc_order_date,
  f.local_order_date,
  f.order_date_key,
  f.store_id,
  f.ip_address,
  f.user_id_db,
  f.email_address,
  f.product_id,
  f.currency,
  r.currency_code,
  f.product_qty,
  f.unit_price,
  f.row_num
FROM  {{ ref('stg_fact_sales') }} f
left join cte_raw_symbol r
on f.currency = r.raw_symbol

)
, cte_sales_join_store_id as (
  SELECT 
  f.order_id,
  f.utc_timestamp,
  f.local_datetime,
  f.utc_order_date,
  f.local_order_date,
  f.order_date_key,
  f.store_id,
  f.ip_address,
  f.user_id_db,
  f.email_address,
  f.product_id,
  f.currency,
  s.country_code,
  s.country_name,
  c.currency_code,
  c.currency_name,
  f.product_qty,
  f.unit_price,
  f.row_num
FROM {{ ref('stg_fact_sales') }} f
left join {{ ref('dim_store') }} s
on f.store_id = s.store_id
left join {{ ref('countries_currencies_full') }} c
on s.country_code = c.country_code_iso2
)
, cte_sales_join_ip_mapping as (
  SELECT 
    f.order_id,
    f.utc_timestamp,
    f.local_datetime,
    f.utc_order_date,
    f.local_order_date,
    f.order_date_key,
    f.store_id,
    f.ip_address,
    f.user_id_db,
    f.email_address,
    f.product_id,
    f.currency,
    i.country_code,
    i.country_name,
    c.currency_code,
    c.currency_name,
    f.product_qty,
    f.unit_price,
    f.row_num
  FROM {{ ref('stg_fact_sales') }} f
  left join {{ ref('stg_ip_location_mapping') }} i
  on f.ip_address = i.ip_address
  left join {{ ref('countries_currencies_full') }} c
  on i.country_code = c.country_code_iso2
)
, cte_currency_code as (
  select
    order_id,
    utc_timestamp,
    local_datetime,
    utc_order_date,
    local_order_date,
    order_date_key,
    store_id,
    ip_address,
    user_id_db,
    email_address,
    product_id,
    currency,
    country_code,
    country_name,
    currency_code as currency_code_old,
    currency_name,
    case
      when store_id = '68' then 'USD'
      when currency_code = 'LTL' then 'EUR'
      else currency_code
    end as currency_code,
    product_qty,
    coalesce(unit_price, 0) as local_unit_price
from cte_sales_join_store_id
)
, cte_sales_join_exchange_rate as (
  select 
  r.date_key,
  c.currency_code as from_currency_code,
  'USD' as to_currency_code,
  r.exchange_rate,
  from {{ ref('fact_snapshot_exchange_rate') }} r
  left join {{ ref('dim_currency') }} c
    on r.from_currency_key = c.currency_key
)
, cte_convert_currency_to_usd as (
select
    f.order_id,
    f.utc_timestamp,
    f.local_datetime,
    f.utc_order_date,
    f.local_order_date,
    f.order_date_key,
    f.store_id,
    f.ip_address,
    f.user_id_db,
    f.email_address,
    f.product_id,
    f.currency_code,
    f.product_qty,
    f.local_unit_price,
    r.from_currency_code,
    f.local_unit_price / r.exchange_rate as usd_unit_price,
    r.to_currency_code,
    r.exchange_rate,
    f.product_qty * f.local_unit_price as local_amount,
    f.product_qty * f.local_unit_price / r.exchange_rate as usd_amount
from cte_currency_code f
left join cte_sales_join_exchange_rate r
  on f.order_date_key = r.date_key
    and f.currency_code = r.from_currency_code
)
, cte_add_store_key as (
select
  f.order_id,
  f.utc_timestamp,
  f.local_datetime,
  f.utc_order_date,
  f.local_order_date,
  f.order_date_key,
  f.store_id,
  s.store_key,
  f.ip_address,
  f.user_id_db,
  f.email_address,
  f.product_id,
  f.currency_code,
  f.product_qty,
  f.local_unit_price,
  f.from_currency_code,
  f.usd_unit_price,
  f.to_currency_code,
  f.exchange_rate,
  f.local_amount,
  f.usd_amount,
from cte_convert_currency_to_usd f
left join {{ ref('dim_store') }} s
  on f.store_id = s.store_id
)
, cte_add_location_key as (
  select
  f.order_id,
  f.utc_timestamp,
  f.local_datetime,
  f.utc_order_date,
  f.local_order_date,
  f.order_date_key,
  f.store_id,
  f.store_key,
  f.ip_address,
  s.location_key,
  f.user_id_db,
  f.email_address,
  f.product_id,
  f.currency_code,
  f.product_qty,
  f.local_unit_price,
  f.from_currency_code,
  f.usd_unit_price,
  f.to_currency_code,
  f.exchange_rate,
  f.local_amount,
  f.usd_amount,
from cte_add_store_key f
left join {{ ref('stg_ip_location_mapping') }} s
  on f.ip_address = s.ip_address
)


, cte_add_scd_customer_key as (
  select
  f.order_id,
  f.utc_timestamp,
  f.local_datetime,
  f.utc_order_date,
  f.local_order_date,
  f.order_date_key,
  f.store_id,
  f.store_key,
  f.ip_address,
  f.location_key,
  s.scd_customer_key,
  f.user_id_db,
  f.email_address,
  f.product_id,
  f.currency_code,
  f.product_qty,
  f.local_unit_price,
  f.from_currency_code,
  f.usd_unit_price,
  f.to_currency_code,
  f.exchange_rate,
  f.local_amount,
  f.usd_amount,
from cte_add_location_key f
left join {{ ref('dim_customer_scd') }} s
  on f.user_id_db = s.user_id_db
    and f.utc_timestamp >= s.start_utc_timestamp
    and f.utc_timestamp < s.end_utc_timestamp
)
, cte_add_product_key as (
  select
  f.order_id,
  f.utc_timestamp,
  f.local_datetime,
  f.utc_order_date,
  f.local_order_date,
  f.order_date_key,
  f.store_id,
  f.store_key,
  f.ip_address,
  f.location_key,
  f.scd_customer_key,
  f.user_id_db,
  f.email_address,
  f.product_id,
  s.product_key,
  f.currency_code,
  f.product_qty,
  f.local_unit_price,
  f.from_currency_code,
  f.usd_unit_price,
  f.to_currency_code,
  f.exchange_rate,
  f.local_amount,
  f.usd_amount,
from cte_add_scd_customer_key f
left join {{ ref('dim_product') }} s
  on f.product_id = s.product_id
)
, cte_process_null as (
select
  f.order_id,
  f.utc_timestamp,
  f.local_datetime,
  f.utc_order_date,
  f.local_order_date,
  f.order_date_key,
  coalesce(f.store_id, '-1') as store_id,
  coalesce(f.store_key, -1) as store_key,
  f.ip_address,
  coalesce(f.location_key, -1) as location_key,
  coalesce(f.scd_customer_key, -1) as scd_customer_key,
  coalesce(f.user_id_db, '-1') as user_id_db,
  coalesce(f.email_address, 'Unknown') as email_address,
  coalesce(f.product_id, '-1') as product_id,
  coalesce(f.product_key, -1) as product_key,
  f.currency_code,
  f.product_qty,
  f.local_unit_price,
  f.from_currency_code,
  f.usd_unit_price,
  f.to_currency_code,
  f.exchange_rate,
  f.local_amount,
  f.usd_amount,
from cte_add_product_key f
)

, cte_final_staging as (
select
  cast(
    farm_fingerprint(
      concat(f.order_id,
      cast(local_order_date as string),
      f.user_id_db,
      f.store_id,
      f.product_id)
    ) as int64) as sales_key,
  f.order_id,
  f.order_date_key,
  f.local_order_date,
  f.local_datetime,
  f.utc_timestamp,
  f.scd_customer_key,
  f.location_key,
  f.store_key,
  f.product_key,
  f.from_currency_code as currency_code,
  f.product_qty,
  f.local_unit_price,
  f.usd_unit_price,
  f.local_amount,
  f.usd_amount,
from cte_process_null f
{% if is_incremental() %}
    -- CHỐT CHẶN INCREMENTAL: Chỉ bốc dữ liệu mới về xử lý
    -- Quét ngược về quá khứ 7 ngày (Lookback Window) để bắt các đơn hàng bị nạp muộn (Late-arriving facts)
    -- hoặc các đơn hàng cũ vừa bị cập nhật lại trạng thái ở hệ thống nguồn.
    WHERE utc_timestamp >= (SELECT DATE_SUB(MAX(utc_timestamp), INTERVAL 7 DAY) FROM {{ this }})
{% endif %}
)

{% if is_incremental() %}
select
  old.sales_key,
  old.order_id,
  old.order_date_key,
  old.local_order_date,
  old.local_datetime,
  old.utc_timestamp,
  old.scd_customer_key,
  old.location_key,
  old.store_key,
  old.product_key,
  stg.currency_code,
  stg.product_qty,
  stg.local_unit_price,
  stg.usd_unit_price,
  stg.local_amount,
  stg.usd_amount,
  old.created_at,
  old.created_by,
  current_timestamp() as updated_at,
  session_user() as updated_by
from cte_final_staging stg
inner join {{ this }} old
on stg.order_id = old.order_id
  and stg.order_date_key = old.order_date_key
  and stg.scd_customer_key = old.scd_customer_key
  and stg.product_key = old.product_key

union all
select
  stg.sales_key,
  stg.order_id,
  stg.order_date_key,
  stg.local_order_date,
  stg.local_datetime,
  stg.utc_timestamp,
  stg.scd_customer_key,
  stg.location_key,
  stg.store_key,
  stg.product_key,
  stg.currency_code,
  stg.product_qty,
  stg.local_unit_price,
  stg.usd_unit_price,
  stg.local_amount,
  stg.usd_amount,
  {{ generate_created_columns() }},
  {{ generate_updated_columns() }}
from cte_final_staging stg
left join {{ this }} old
on stg.order_id = old.order_id
  and stg.order_date_key = old.order_date_key
  and stg.scd_customer_key = old.scd_customer_key
  and stg.product_key = old.product_key
where old.order_id is null
{% else %}
select
  f.sales_key,
  f.order_id,
  f.order_date_key,
  f.local_order_date,
  f.local_datetime,
  f.utc_timestamp,
  f.scd_customer_key,
  f.location_key,
  f.store_key,
  f.product_key,
  f.currency_code,
  f.product_qty,
  f.local_unit_price,
  f.usd_unit_price,
  f.local_amount,
  f.usd_amount,
  {{ generate_created_columns() }},
  {{ generate_updated_columns() }}
from cte_final_staging f
{% endif %}
