{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['core','non_realtime','reorg'],
    persist_docs ={ "relation": true,
    "columns": true }
) }}

WITH xdai_base AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        identifier,
        from_address,
        to_address,
        xdai_value,
        _call_id,
        _inserted_timestamp,
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
        ) AS xdai_value_precise,
        tx_position,
        trace_index
    FROM
        {{ ref('silver__traces') }}
    WHERE
        xdai_value > 0
        AND tx_status = 'SUCCESS'
        AND trace_status = 'SUCCESS'
        AND TYPE NOT IN (
            'DELEGATECALL',
            'STATICCALL'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '72 hours'
    FROM
        {{ this }}
)
{% endif %}
),
tx_table AS (
    SELECT
        block_number,
        tx_hash,
        from_address AS origin_from_address,
        to_address AS origin_to_address,
        origin_function_signature
    FROM
        {{ ref('silver__transactions') }}
    WHERE
        tx_hash IN (
            SELECT
                DISTINCT tx_hash
            FROM
                xdai_base
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '72 hours'
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    tx_hash AS tx_hash,
    block_number AS block_number,
    block_timestamp AS block_timestamp,
    identifier AS identifier,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    from_address AS xdai_from_address,
    to_address AS xdai_to_address,
    xdai_value AS amount,
    xdai_value_precise_raw AS amount_precise_raw,
    xdai_value_precise AS amount_precise,
    ROUND(
        xdai_value * price,
        2
    ) AS amount_usd,
    _call_id,
    _inserted_timestamp,
    tx_position,
    trace_index
FROM
    xdai_base A
    LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
    ON DATE_TRUNC(
        'hour',
        A.block_timestamp
    ) = HOUR
    AND token_address = LOWER('0xe91D153E0b41518A2Ce8Dd3D7944Fa863463a97d')
    JOIN tx_table USING (
        tx_hash,
        block_number
    )
