{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    A.tx_hash AS tx_hash,
    A.block_number AS block_number,
    A.block_timestamp AS block_timestamp,
    A.identifier AS identifier,
    tx.from_address AS origin_from_address,
    tx.to_address AS origin_to_address,
    origin_function_signature,
    A.from_address AS xdai_from_address,
    A.to_address AS xdai_to_address,
    A.xdai_value AS amount,
    ROUND(
        A.xdai_value * price,
        2
    ) AS amount_usd,
    _call_id
FROM
    {{ ref('silver__traces') }} A
    LEFT JOIN {{ source('ethereum','fact_hourly_token_prices') }}
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = HOUR
    AND token_address = LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F')
    LEFT JOIN {{ ref('silver__transactions') }}
    tx
    ON A.tx_hash = tx.tx_hash
WHERE
    A.xdai_value > 0
    AND A.tx_status = 'SUCCESS'
    AND A.gas_used IS NOT NULL
