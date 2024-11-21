{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    1 AS tx_position,
    --new column
    trace_index,
    from_address,
    to_address,
    input,
    output,
    TYPE,
    '0x' AS trace_address,
    --new column
    sub_traces,
    DATA,
    xdai_value AS VALUE,
    IFNULL(
        xdai_value_precise_raw,
        '0'
    ) AS value_precise_raw,
    IFNULL(
        xdai_value_precise,
        '0'
    ) AS value_precise,
    '0x' AS value_hex,
    --new column
    gas,
    gas_used,
    '0x' AS origin_from_address,
    -- new column
    '0x' AS origin_to_address,
    -- new column
    '0x' AS origin_function_signature,
    -- new column
    TRUE AS trace_succeeded,
    -- new column
    error_reason,
    '0x' AS revert_reason,
    -- new column
    TRUE AS tx_succeeded,
    -- new column
    COALESCE (
        traces_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'trace_index']
        ) }}
    ) AS fact_traces_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp,
    identifier,
    --deprecate
    tx_status,
    --deprecate
    trace_status --deprecate
FROM
    (
        SELECT
            tx_hash,
            block_number,
            block_timestamp,
            from_address,
            to_address,
            xdai_value,
            gas,
            gas_used,
            input,
            output,
            TYPE,
            identifier,
            DATA,
            tx_status,
            sub_traces,
            trace_status,
            error_reason,
            trace_index,
            REPLACE(
                COALESCE(
                    DATA :value :: STRING,
                    DATA :action :value :: STRING
                ),
                '0x'
            ) AS hex,
            to_varchar(TO_NUMBER(hex, REPEAT('X', LENGTH(hex)))) AS xdai_value_precise_raw,
            IFF(LENGTH(xdai_value_precise_raw) > 18, LEFT(xdai_value_precise_raw, LENGTH(xdai_value_precise_raw) - 18) || '.' || RIGHT(xdai_value_precise_raw, 18), '0.' || LPAD(xdai_value_precise_raw, 18, '0')) AS rough_conversion,
            IFF(
                POSITION(
                    '.000000000000000000' IN rough_conversion
                ) > 0,
                LEFT(rough_conversion, LENGTH(rough_conversion) - 19),
                REGEXP_REPLACE(
                    rough_conversion,
                    '0*$',
                    ''
                )
            ) AS xdai_value_precise,
            traces_id,
            inserted_timestamp,
            modified_timestamp
        FROM
            {{ ref('silver__traces') }}
    )
