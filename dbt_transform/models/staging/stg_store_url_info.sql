{{
    config(
        materialized = 'view',
        schema = 'staging'
    )
}}

with store_id_and_current_url as (
    select distinct
        cast(store_id as int64) as store_id,
        case
            when current_url like any ('%www.glamira.%', '%www.ring%')
                then TRIM(NET.HOST(current_url),'.')
            when current_url like '%glamira.local/gl%'
                then REGEXP_EXTRACT(current_url, r'^https?://([^/]+/[^/]+)')
        end as hostname_or_extracted_local_url
    from {{ source('glamira_sources', 'summary_raw') }}
    where
        store_id is not null and store_id <> ''
        and current_url like any ('%www.glamira.%', '%www.ring%', '%glamira.local/gl%')
        and current_url not like all ('%google%', '%dev%', '%web%', '%chase%', '%trans%', '%test%', '%click%')
    order by store_id
)

select
    *
from store_id_and_current_url
where hostname_or_extracted_local_url is not null