{{ config (
    materialized = 'view'
) }}
{{ streamline_external_table_fr_query(
    model = "debug_traceBlockByNumber",
    partition_function = "CAST(SPLIT_PART(SPLIT_PART(file_name, '/', 3), '_', 1) AS INTEGER )",
    partition_name = "_partition_by_block_id",
    unique_key = "block_number"
) }}