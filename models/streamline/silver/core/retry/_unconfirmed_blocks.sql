{{ config (
    materialized = "ephemeral"
) }}

WITH lookback AS (

    SELECT
        block_number
    FROM
        {{ ref("_block_lookback") }}
)
SELECT
    DISTINCT cb.block_number AS block_number
FROM
    {{ ref("silver__confirmed_blocks") }}
    cb
    LEFT JOIN {{ ref("silver__transactions") }}
    txs
    ON cb.block_number = txs.block_number
    AND cb.block_hash = txs.block_hash
    AND cb.tx_hash = txs.tx_hash
    AND txs._inserted_timestamp >= DATEADD('hour', -84, SYSDATE())
    AND txs.block_number >= (
        SELECT
            block_number
        FROM
            lookback
    )
WHERE
    txs.tx_hash IS NULL
    AND cb.block_number >= (
        SELECT
            block_number
        FROM
            lookback
    )
    AND cb._inserted_timestamp >= DATEADD('hour', -84, SYSDATE())
