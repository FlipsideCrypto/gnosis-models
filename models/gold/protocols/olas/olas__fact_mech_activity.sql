{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'OLAS, AUTONOLAS, VALORY',
    'PURPOSE': 'AI, SERVICES, MECH' } } }
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_index,
    event_name,
    sender_address,
    request_id,
    data_payload,
    prompt_link AS metadata_link,
    mech_requests_id AS fact_mech_activity_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver_olas__mech_requests') }}
UNION ALL
SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_index,
    event_name,
    sender_address,
    request_id,
    data_payload,
    deliver_link AS metadata_link,
    _log_id,
    _inserted_timestamp,
    mech_delivers_id AS fact_mech_activity_id,
    inserted_timestamp,
    modified_timestamp
FROM
    {{ ref('silver_olas__mech_delivers') }}
