{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'OLAS, AUTONOLAS, VALORY',
    'PURPOSE': 'AI, SERVICES' } } }
) }}

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
    s.service_id,
    m.name,
    m.description,
    s.reward_unadj,
    s.reward_adj AS reward,
    s.epoch,
    s.epoch_length,
    s.available_rewards_unadj AS total_available_rewards_unadj,
    s.available_rewards_adj AS total_available_rewards,
    s.program_name,
    s.service_checkpoint_id AS ez_service_checkpoints_id,
    s.inserted_timestamp,
    s.modified_timestamp
FROM
    {{ ref('silver_olas__service_checkpoint') }}
    s
    LEFT JOIN {{ ref('silver_olas__registry_metadata') }}
    m
    ON s.service_id = m.registry_id
