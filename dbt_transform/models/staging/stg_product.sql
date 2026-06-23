{{
  config(
    materialized = 'view',
    schema = 'staging'
    )
}}

select
    cast(trim(product_id) as string) as product_id,
    cast(trim(name) as string) as product_name,
    cast(trim(sku) as string) as product_sku,
    cast(trim(attribute_set_id) as string) as attribute_set_id,
    cast(trim(attribute_set) as string) as attribute_set_name,
    cast(trim(type_id) as string) as packaging_type,
    cast(price as numeric) as vnd_unit_price,
    cast(min_price as numeric) as vnd_min_price,
    cast(max_price as numeric) as vnd_max_price,
    cast(nullif(trim(collection_id), '') as string) as collection_id,
    cast(nullif(trim(collection), '') as string) as collection_name,
    cast(nullif(trim(product_type_value), '') as string) as product_type_id,
    cast(nullif(trim(product_type), '') as string) as product_type_name,
    cast(nullif(trim(category), '') as string) as category_id,
    cast(nullif(trim(category_name), '') as string) as category_name,
    cast(nullif(trim(gender), '') as string) as gender
from {{ source('glamira_sources', 'products') }}
where product_id is not null or trim(product_id) <> ''