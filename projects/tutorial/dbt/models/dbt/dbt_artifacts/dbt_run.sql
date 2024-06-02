{{ config(
    tags=['dbt_artifacts'],
    contract={'enforced': true},
    on_schema_change='fail',
    materialized='incremental',
    incremental_strategy='append',
    unique_key='id'
) }}

with final as (
    select 1
)

select
    cityHash64('') as id,
    generateUUIDv4() as invocation_id,
    '' as node_id,
    '' as thread_id,
    '' as resource_type,
    '' as schema,
    '' as name,
    '' as alias,
    '' as materialization,
    '' as run_type,
    cast(now(), 'DateTime64(6)') as run_start_time,
    cast(null, 'Nullable(DateTime64(6))') as compile_start_time,
    cast(null, 'Nullable(UInt64)') as compile_duration,
    cast(null, 'Nullable(DateTime64(6))') as execute_start_time,
    cast(null, 'Nullable(UInt64)') as execute_duration,
    '' as status,
    '' as message,
    cast(null, 'Nullable(UInt64)') as rows_affected,
    '' as adapter_response
from final
where 1 = 0
