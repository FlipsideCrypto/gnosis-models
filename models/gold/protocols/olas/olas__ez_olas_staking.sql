{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'OLAS, AUTONOLAS, VALORY',
    'PURPOSE': 'STAKING, DEPOSIT, WITHDRAWAL' } } }
) }}

WITH base_evt AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_name,
        sender_address AS staker_address,
        amount_unadj,
        amount_adj,
        token_symbol,
        token_address,
        program_name,
        olas_staking_deposits_id AS fact_olas_staking_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver_olas__olas_staking_deposits') }}
    UNION ALL
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        event_index,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_name,
        withdrawer_address AS staker_address,
        amount_unadj,
        amount_adj,
        token_symbol,
        token_address,
        program_name,
        olas_staking_withdrawals_id AS fact_olas_staking_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver_olas__olas_staking_withdrawals') }}
)
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    event_index,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    withdrawer_address AS staker_address,
    amount_unadj,
    amount_adj AS amount,
    ROUND(
        amount_adj * p.price,
        2
    ) AS amount_usd,
    token_symbol,
    token_address,
    program_name,
    fact_olas_staking_id,
    inserted_timestamp,
    modified_timestamp
FROM
    base_evt b
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p
    ON b.token_address = p.token_address
    AND DATE_TRUNC(
        'hour',
        block_timestamp
    ) = p.hour
