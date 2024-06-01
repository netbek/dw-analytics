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
    '' as thread_id,
    '' as node_id,
    '' as schema,
    '' as name,
    '' as alias,
    '' as materialization,
    '' as run_type,
    cast(now(), 'DateTime64(6)') as run_start_time,
    cast(now(), 'DateTime64(6)') as compile_start_time,
    {# cast(null, 'Nullable(DateTime64(6))') as compile_end_time, #}
    {# cast(null, 'Nullable(DateTime64(6))') as query_start_time, #}
    cast(now(), 'DateTime64(6)') as query_end_time,
    cast(0, 'Float32') as execution_interval, -- 2024-06-01: Database driver does not support interval type
    '' as status,
    '' as message,
    cast(null, 'Nullable(UInt64)') as rows_affected,
    '' as adapter_response
from final
where 1 = 0
