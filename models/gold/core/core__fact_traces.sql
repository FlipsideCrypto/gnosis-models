{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    tx_hash,
    block_number,
    block_timestamp,
    from_address,
    to_address,
    xdai_value,
    IFNULL(
        xdai_value_precise_raw,
        '0'
    ) AS xdai_value_precise_raw,
    IFNULL(
        xdai_value_precise,
        '0'
    ) AS xdai_value_precise,
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
    trace_index
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
            ) AS xdai_value_precise
        FROM
            {{ ref('silver__traces') }}
    )