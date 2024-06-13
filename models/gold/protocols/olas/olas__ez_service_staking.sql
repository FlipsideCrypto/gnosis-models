{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'OLAS, AUTONOLAS, VALORY',
    'PURPOSE': 'SERVICES, STAKING, DEPOSIT, WITHDRAWAL' } } }
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
        service_id,
        owner_address,
        multisig_address,
        epoch,
        nonces,
        program_name,
        services_staked_id AS ez_service_staking_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver_olas__services_staked') }}
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
        service_id,
        owner_address,
        multisig_address,
        epoch,
        nonces,
        program_name,
        services_unstaked_id AS ez_service_staking_id,
        inserted_timestamp,
        modified_timestamp
    FROM
        {{ ref('silver_olas__services_unstaked') }}
)
SELECT
    s.block_number,
    s.block_timestamp,
    s.tx_hash,
    s.origin_function_signature,
    s.origin_from_address,
    s.origin_to_address,
    s.contract_address,
    s.event_index,
    s.event_name,
    s.owner_address,
    s.multisig_address,
    s.service_id,
    m.name,
    m.description,
    s.epoch,
    s.nonces,
    s.program_name,
    s.ez_service_staking_id,
    s.inserted_timestamp,
    s.modified_timestamp
FROM
    base_evt s
    LEFT JOIN {{ ref('silver_olas__registry_metadata') }}
    m
    ON s.service_id = m.registry_id
