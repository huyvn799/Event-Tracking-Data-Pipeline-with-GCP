{{
    config(
        materialized = 'incremental',
        schema = 'core',
        cluster_by = ['store_key', 'store_id'],
        unique_key = ['store_id'],
        incremental_strategy = 'merge',
        merge_update_columns = ['country_code', 'country_name', 'updated_at', 'updated_by']
    )
}}
with cte_all_store_ids as (
    select
        distinct store_id
    from {{ ref('stg_store_url_info') }}
    where store_id is not null
)
, cte_hostname_url as (
    select distinct
        store_id,
        REGEXP_EXTRACT(hostname_or_extracted_local_url, r'\.([a-zA-Z]+)$') as country_code_from_hostname
    from {{ ref('stg_store_url_info') }}
    where hostname_or_extracted_local_url not like all ('%glamira.local%')
)

, cte_local_url as (
    select distinct
        store_id,
        RIGHT(REGEXP_EXTRACT(hostname_or_extracted_local_url, r'([^/]+)$'), 2) as country_code_from_local_url
    from {{ ref('stg_store_url_info') }}
    where hostname_or_extracted_local_url like any ('%glamira.local/%')
)

, cte_join_by_store_id as (
    select distinct
        a.store_id as store_id,
        b.country_code_from_hostname as country_code_from_hostname,
        c.country_code_from_local_url as country_code_from_local_url
    from cte_all_store_ids a
    left join cte_hostname_url b
        on a.store_id = b.store_id
    left join cte_local_url c
        on a.store_id = c.store_id
    order by a.store_id
)
, cte_get_country_code as (
    select distinct
        store_id,
        upper(cast(
            case
                when country_code_from_local_url in ('us', 'gb')
                    then coalesce(country_code_from_local_url, country_code_from_hostname)
                else coalesce(country_code_from_hostname, country_code_from_local_url)
            end as string)) as country_code
    from cte_join_by_store_id
    order by store_id
)
, cte_store_staging as (
    select
        {# {{ dbt_utils.generate_surrogate_key(['store_id', 'country_code']) }} as store_key, #}
        farm_fingerprint(cast(t.store_id as string)) as store_key,
        cast(t.store_id as string) as store_id,
        cast(t.country_code as string) as country_code,
        cast(coalesce(stg.country_name, 'INTERNATIONAL') as string) as country_name
    from cte_get_country_code t
    left join {{ ref('countries_currencies_full') }} as stg
        on t.country_code = stg.country_code_iso2
)

, cte_store_with_audit_and_incremental as (
    select
        stg.store_key,
        stg.store_id,
        stg.country_code,
        stg.country_name,

    {% if is_incremental() %} -- Xử lý created columns
        coalesce(old.created_at, current_timestamp()) as created_at,
        coalesce(old.created_by, session_user()) as created_by,
    {% else %} -- full-refresh sẽ tạo mới tất cả các bản ghi, do đó cần có created columns
        current_timestamp() as created_at,
        session_user() as created_by,
    {% endif %}

    {% if is_incremental() %} -- Xử lý updated columns
        {% set condition = "stg.country_code <> old.country_code or stg.country_name <> old.country_name" %}
        {{ get_updated_value(condition, "current_timestamp()", "old.updated_at") }} as updated_at,
        {{ get_updated_value(condition, "session_user()", "old.updated_by") }} as updated_by
    {% else %}
        current_timestamp() as updated_at,
        session_user() as updated_by
    {% endif%}

    from cte_store_staging stg
    {% if is_incremental() %}
        left join {{ this }} old
            on stg.store_key = old.store_key
    {% else %}
    union all
    select
        -1 as store_key,
        '-1' as store_id,
        'Unknown' as country_code,
        'Unknown' as country_name,
        {{ generate_created_columns() }},
        {{ generate_updated_columns() }}
    {% endif %}
)

select
    *
from cte_store_with_audit_and_incremental
