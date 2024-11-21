{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
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
    topic_0,
    topic_1,
    topic_2,
    topic_3,
    'Deliver' AS event_name,
    DATA,
    regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
    CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS sender_address,
    utils.udf_hex_to_int(
        segmented_data [0] :: STRING
    ) AS request_id,
    CONCAT(
        '0x',
        segmented_data [3] :: STRING
    ) AS data_payload,
    CONCAT(
        'https://gateway.autonolas.tech/ipfs/f01701220',
        segmented_data [3] :: STRING,
        '/',
        request_id
    ) AS delivery_link,
    CONCAT(
        tx_hash :: STRING,
        '-',
        event_index :: STRING
    ) AS _log_id,
    modified_timestamp AS _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS mech_delivers_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    {{ ref('core__fact_event_logs') }}
WHERE
    contract_address = '0x77af31de935740567cf4ff1986d04b2c964a786a' --AgentMech
    AND topic_0 = '0x0cd979445339c62199996f208428d987b1cea24d18e62b79ec24d94b636e8b70' --Deliver
    AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
