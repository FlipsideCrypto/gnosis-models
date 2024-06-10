{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'OLAS, AUTONOLAS, VALORY',
    'PURPOSE': 'SERVICES, STAKING, DEPOSIT, WITHDRAWAL' } } }
) }}

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
