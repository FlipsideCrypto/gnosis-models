{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_number','platform'],
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH 
aave AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        aave_token AS protocol_market,
        aave_market AS token_address,
        symbol AS token_symbol,
        amount_unadj,
        amount,
        depositor_address,
        'Aave V3' AS platform,
        'polygon' AS blockchain,
        _LOG_ID,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__aave_withdraws') }}

{% if is_incremental() and 'aave' not in var('HEAL_CURATED_MODEL') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
spark AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        spark_token AS protocol_market,
        spark_market AS token_address,
        symbol AS token_symbol,
        amount_unadj,
        amount,
        depositor_address,
        'spark V3' AS platform,
        'polygon' AS blockchain,
        _LOG_ID,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__spark_withdraws') }}

{% if is_incremental() and 'spark' not in var('HEAL_CURATED_MODEL') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
agave AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        agave_token AS protocol_market,
        agave_market AS token_address,
        symbol AS token_symbol,
        amount_unadj,
        amount,
        depositor_address,
        'agave V3' AS platform,
        'polygon' AS blockchain,
        _LOG_ID,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__agave_withdraws') }}

{% if is_incremental() and 'agave' not in var('HEAL_CURATED_MODEL') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
realT AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        realT_token AS protocol_market,
        realT_market AS token_address,
        symbol AS token_symbol,
        amount_unadj,
        amount,
        depositor_address,
        'realT V3' AS platform,
        'polygon' AS blockchain,
        _LOG_ID,
        _INSERTED_TIMESTAMP
    FROM
        {{ ref('silver__realT_withdraws') }}

{% if is_incremental() and 'realT' not in var('HEAL_CURATED_MODEL') %}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '36 hours'
        FROM
            {{ this }}
    )
{% endif %}
),
withdraws_union as (
    SELECT
        *
    FROM
        aave
    UNION ALL
    SELECT
        *
    FROM
        agave
    UNION ALL
    SELECT
        *
    FROM
        spark
    UNION ALL
    SELECT
        *
    FROM
        realT
),

FINAL AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        A.contract_address,
        CASE
            WHEN platform = 'Compound V3' THEN 'WithdrawCollateral'
            ELSE 'Withdraw'
        END AS event_name,
        protocol_market,
        depositor_address AS depositor,
        A.token_address,
        A.token_symbol,
        amount_unadj,
        amount,
        ROUND(
            amount * price,
            2
        ) AS amount_usd,
        platform,
        blockchain,
        A._log_id,
        A._inserted_timestamp
    FROM
        withdraws_union A
        LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
        p
        ON A.token_address = p.token_address
        AND DATE_TRUNC(
            'hour',
            block_timestamp
        ) = p.hour
        LEFT JOIN {{ ref('silver__contracts') }} C
        ON A.token_address = C.contract_address
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS complete_lending_withdraws_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
