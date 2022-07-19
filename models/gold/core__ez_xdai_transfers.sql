{{ config(
    materialized = 'incremental',
    persist_docs ={ "relation": true,
    "columns": true },
    unique_key = '_call_id',
    cluster_by = ['block_timestamp::DATE']
) }}

WITH eth_base AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        from_address,
        to_address,
        xdai_value,
        identifier,
        _call_id,
        _inserted_timestamp,
        input
    FROM
        {{ ref('silver__traces') }}
    WHERE
        xdai_value > 0
        AND tx_status = 'SUCCESS'
        AND gas_used IS NOT NULL

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
),
dai_price AS (
    SELECT
        HOUR,
        AVG(price) AS dai_price
    FROM
        {{ source(
            'ethereum',
            'fact_hourly_token_prices'
        ) }}
    WHERE
        token_address = LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F')
        AND HOUR :: DATE IN (
            SELECT
                DISTINCT block_timestamp :: DATE
            FROM
                eth_base
        )
    GROUP BY
        HOUR
),
tx_table AS (
    SELECT
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
                eth_base
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) :: DATE - 2
    FROM
        {{ this }}
)
{% endif %}
)
SELECT
    A.tx_hash AS tx_hash,
    A.block_number AS block_number,
    A.block_timestamp AS block_timestamp,
    A.identifier AS identifier,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    A.from_address AS eth_from_address,
    A.to_address AS eth_to_address,
    A.xdai_value AS amount,
    ROUND(
        A.xdai_value * eth_price,
        2
    ) AS amount_usd,
    _call_id,
    _inserted_timestamp
FROM
    eth_base A
    LEFT JOIN eth_price
    ON DATE_TRUNC(
        'hour',
        block_timestamp
    ) = HOUR
    LEFT JOIN tx_table
    ON A.tx_hash = tx_table.tx_hash
