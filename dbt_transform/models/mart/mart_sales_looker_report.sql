{{
    config(
        materialized='table',
        tags=['looker_bi'],
        cluster_by=['order_date']
    )
}}

WITH sales AS (
    SELECT * FROM {{ ref('fact_sales_order_detail') }}
),

dates AS (
    SELECT * FROM {{ ref('dim_date') }}
),

products AS (
    SELECT * FROM {{ ref('dim_product') }}
),

locations AS (
    SELECT * FROM {{ ref('dim_location') }}
),

stores AS (
    SELECT * FROM {{ ref('dim_store') }}
),

currencies AS (
    SELECT * FROM {{ ref('dim_currency') }}
),

scd_customers AS (
    SELECT * FROM {{ ref('dim_customer_scd') }}
)

SELECT
    -- ====== 1. THỜI GIAN (Time-based Trends) ======
    s.order_id,
    s.local_datetime AS order_datetime,
    d.full_date AS order_date,
    d.year_num AS year,
    d.quarter_name AS quarter_name,
    d.month_num AS month_num,
    d.full_month_name AS month_name,
    d.full_day_name AS day_name,
    d.is_weekend,

    -- ====== 2. SẢN PHẨM (Product Performance) ======
    p.product_id,
    p.product_name,
    p.product_sku,
    p.category_name AS product_category,
    p.collection_name AS product_collection,
    p.gender AS product_gender,

    -- ====== 3. ĐỊA LÝ & CỬA HÀNG (Geographic Distribution) ======
    l.country_name AS customer_country,
    l.region AS customer_region,
    l.city AS customer_city,
    st.store_id,
    st.country_name AS store_country,

    -- ====== 4. THÔNG TIN KHÁCH HÀNG (Customer Info) ======
    cus.user_id_db AS customer_id,
    cus.email_address AS customer_email,

    -- ====== 5. TIỀN TỆ & DOANH THU (Revenue Analysis) ======
    cur.currency_code,
    cur.currency_name,
    s.product_qty AS quantity_sold,
    s.usd_unit_price AS unit_price,
    -- Ưu tiên dùng usd_amount để tổng hợp doanh thu toàn cầu không bị lệch tỷ giá
    s.usd_amount AS sales_amount

FROM sales s
LEFT JOIN dates d ON s.order_date_key = d.date_key
LEFT JOIN products p ON s.product_key = p.product_key
LEFT JOIN stores st ON s.store_key = st.store_key
LEFT JOIN locations l ON s.location_key = l.location_key
LEFT JOIN scd_customers cus ON s.scd_customer_key = cus.scd_customer_key
LEFT JOIN currencies cur ON s.currency_key = cur.currency_key