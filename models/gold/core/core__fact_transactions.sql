{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}


SELECT
block_number,
    block_timestamp,
    block_hash,
    tx_hash,
    nonce,
    POSITION,
    origin_function_signature,
    from_address,
    to_address,
    xdai_value,
    xdai_value_precise,
    xdai_value_precise_raw,
    tx_fee,
    tx_fee_precise,
    gas_price,
    effective_gas_price,
    gas_limit,
    gas_used,
    cumulative_gas_used,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    input_data,
    status,
    r,
    s,
    v,
    tx_type    
FROM
(SELECT
    block_number,
    block_timestamp,
    block_hash,
    tx_hash,
    nonce,
    POSITION,
    origin_function_signature,
    from_address,
    to_address,
    VALUE AS xdai_value,
    tx_fee,
    tx_fee_precise,
    gas_price,
    effective_gas_price,
    gas AS gas_limit,
    gas_used,
    cumulative_gas_used,
    max_fee_per_gas,
    max_priority_fee_per_gas,
    input_data,
    tx_status AS status,
    r,
    s,
    v,
    tx_type,
    to_varchar(
                TO_NUMBER(REPLACE(DATA :value :: STRING, '0x'), REPEAT('X', LENGTH(REPLACE(DATA :value :: STRING, '0x'))))
            ) AS xdai_value_precise_raw,
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
    {{ ref('silver__transactions') }}
)