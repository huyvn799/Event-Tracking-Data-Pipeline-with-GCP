{{
    config(
        materialized = 'incremental',
        schema = 'core',
        unique_key = 'product_id',
        incremental_strategy = 'merge',
        merge_update_columns = [
            'product_name',
            'product_sku',
            'attribute_set_id',
            'attribute_set_name',
            'packaging_type',
            'collection_id',
            'collection_name',
            'product_type_id',
            'product_type_name',
            'category_id',
            'category_name',
            'gender',
            'vnd_unit_price',
            'vnd_min_price',
            'vnd_max_price',
            'updated_at',
            'updated_by'
        ]
    )
}}
with cte_product_staging as (
  select
      farm_fingerprint(cast(product_id as string)) as product_key,
      product_id as product_id,
      coalesce(product_name, 'Unknown') as product_name,
      coalesce(product_sku, 'Unknown') as product_sku,
      coalesce(attribute_set_id, '-1') as attribute_set_id,
      coalesce(attribute_set_name, 'Unknown') as attribute_set_name,
      coalesce(packaging_type, 'Unknown') as packaging_type,
      {% set condition = "collection_id is null or collection_id in ('0', '-1')"%}
      {{ get_updated_value(condition, "'-1'", "coalesce(collection_id, '-1')") }} as collection_id,
      {{ get_updated_value(condition, "'Unknown'", "coalesce(collection_name, 'Unknown')") }} as collection_name,
      {% set condition = "product_type_id is null or product_type_id in ('0', '-1')"%}
      {{ get_updated_value(condition, "'-1'", "coalesce(product_type_id, '-1')") }} as product_type_id,
      {{ get_updated_value(condition, "'Unknown'", "coalesce(product_type_name, 'Unknown')") }} as product_type_name,
      {% set condition = "category_id is null or category_id in ('0', '-1')"%}
      {{ get_updated_value(condition, "'-1'", "coalesce(category_id, '-1')") }} as category_id,
      {{ get_updated_value(condition, "'Unknown'", "coalesce(category_name, 'Unknown')") }} as category_name,
      case
        when gender = 'false' then 'Uncategorized'
        else coalesce(gender, 'Unknown')
      end as gender,
      coalesce(vnd_unit_price, 0) as vnd_unit_price,
      coalesce(vnd_min_price, 0) as vnd_min_price,
      coalesce(vnd_max_price, 0) as vnd_max_price
  from {{ ref('stg_product') }}
)

, cte_product_with_audit_and_incremental as (
  {% if is_incremental() %}
  select
    old.product_key,
    old.product_id,
    stg.product_name,
    stg.product_sku,
    stg.attribute_set_id,
    stg.attribute_set_name,
    stg.packaging_type,
    stg.collection_id,
    stg.collection_name,
    stg.product_type_id,
    stg.product_type_name,
    stg.category_id,
    stg.category_name,
    stg.gender,
    stg.vnd_unit_price,
    stg.vnd_min_price,
    stg.vnd_max_price,
    old.created_at,
    old.created_by,
    current_timestamp() as updated_at,
    session_user() as updated_by
  from cte_product_staging stg
  inner join {{ this }} old
    on stg.product_id = old.product_id
  where stg.product_name <> old.product_name
    or stg.product_sku <> old.product_sku
    or stg.attribute_set_id <> old.attribute_set_id
    or stg.attribute_set_name <> old.attribute_set_name
    or stg.packaging_type <> old.packaging_type
    or stg.collection_id <> old.collection_id
    or stg.collection_name <> old.collection_name
    or stg.product_type_id <> old.product_type_id
    or stg.product_type_name <> old.product_type_name
    or stg.category_id <> old.category_id
    or stg.category_name <> old.category_name
    or stg.gender <> old.gender
    or stg.vnd_unit_price <> old.vnd_unit_price
    or stg.vnd_min_price <> old.vnd_min_price
    or stg.vnd_max_price <> old.vnd_max_price

  union all

  select
    farm_fingerprint(cast(stg.product_id as string)) as product_key,
    stg.product_id,
    stg.product_name,
    stg.product_sku,
    stg.attribute_set_id,
    stg.attribute_set_name,
    stg.packaging_type,
    stg.collection_id,
    stg.collection_name,
    stg.product_type_id,
    stg.product_type_name,
    stg.category_id,
    stg.category_name,
    stg.gender,
    stg.vnd_unit_price,
    stg.vnd_min_price,
    stg.vnd_max_price,
    {{ generate_created_columns() }},
    {{ generate_updated_columns() }}
  from cte_product_staging stg
  left join {{ this }} old
    on stg.product_id = old.product_id
  where old.product_id is null
      
  {% else %}
  select
    product_key,
    product_id,
    product_name,
    product_sku,
    attribute_set_id,
    attribute_set_name,
    packaging_type,
    collection_id,
    collection_name,
    product_type_id,
    product_type_name,
    category_id,
    category_name,
    gender,
    vnd_unit_price,
    vnd_min_price,
    vnd_max_price,
    {{ generate_created_columns() }},
    {{ generate_updated_columns() }}
  from cte_product_staging stg
  union all
  select
    -1 as product_key,
    '-1'as product_id,
    'Unknown'as product_name,
    'Unknown'as product_sku,
    '-1'as attribute_set_id,
    'Unknown'as attribute_set_name,
    'Unknown'as packaging_type,
    '-1'as collection_id,
    'Unknown'as collection_name,
    '-1'as product_type_id,
    'Unknown'as product_type_name,
    '-1'as category_id,
    'Unknown'as category_name,
    'Unknown'as gender,
    0 as vnd_unit_price,
    0 as vnd_min_price,
    0 as vnd_max_price,
    {{ generate_created_columns() }},
    {{ generate_updated_columns() }}
  {% endif %}
)

select 
  *
from cte_product_with_audit_and_incremental