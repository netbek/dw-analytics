# Patches

- columns_in_query.diff - Prevents a data scan when fetching the schema. Tested with native driver (clickhouse-driver==0.2.6, dbt-clickhouse==1.6.2).
- format_columns.diff - Fixes the mismatch between the data type inferred by SQL and defined in YAML, e.g. Int32 in SQL vs. Nullable(Int32) in YAML. Both must be the latter. Tested with native driver (clickhouse-driver==0.2.6, dbt-clickhouse==1.6.2).
