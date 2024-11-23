{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH decoded_evt AS (

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
        topics [0] :: STRING AS topic_0,
        topics [1] :: STRING AS topic_1,
        topics [2] :: STRING AS topic_2,
        topics [3] :: STRING AS topic_3,
        decoded_flat,
        CASE
            WHEN contract_address = '0xee9f19b5df06c7e8bfc7b28745dcf944c504198a' THEN 'Alpha'
            WHEN contract_address = '0x43fb32f25dce34eb76c78c7a42c8f40f84bcd237' THEN 'Coastal'
            WHEN contract_address = '0x2ef503950be67a98746f484da0bbada339df3326' THEN 'Alpine'
            WHEN contract_address = '0x5add592ce0a1b5dcecebb5dcac086cd9f9e3ea5c' THEN 'Everest'
            WHEN contract_address = '0x5344b7dd311e5d3dddd46a4f71481bd7b05aaa3e' THEN 'Quickstart Beta - Expert'
            WHEN contract_address = '0x389b46c259631acd6a69bde8b6cee218230bae8c' THEN 'Quickstart Beta - Hobbyist'
            WHEN contract_address = '0xef44fb0842ddef59d37f85d61a1ef492bba6135d' THEN 'Pearl Beta'
        END AS program_name,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        contract_address IN (
            '0xee9f19b5df06c7e8bfc7b28745dcf944c504198a',
            --StakingProxy (Alpha)
            '0x43fb32f25dce34eb76c78c7a42c8f40f84bcd237',
            --ServiceStakingTokenMechUsage (Coastal)
            '0x2ef503950be67a98746f484da0bbada339df3326',
            --ServiceStakingTokenMechUsage (Alpine)
            '0x5add592ce0a1b5dcecebb5dcac086cd9f9e3ea5c',
            --ServiceStakingTokenMechUsage (Everest)
            '0x5344b7dd311e5d3dddd46a4f71481bd7b05aaa3e',
            --Quickstart Beta - Expert
            '0x389b46c259631acd6a69bde8b6cee218230bae8c',
            --Quickstart Beta - Hobbyist
            '0xef44fb0842ddef59d37f85d61a1ef492bba6135d' --Pearl Beta
        )
        AND topic_0 IN (
            '0x48b735a18ed32318d316214e41387be29c52e29df4598f2b8e40fa843be3f940',
            '0x06a98bdd4732811ab3214800ed1ada2dce66a2bce301d250c3ca7d6b461ee666',
            '0x21d81d5d656869e8ce3ba8d65526a2f0dbbcd3d36f5f9999eb7c84360e45eced'
        ) --Checkpoint
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
),
checkpoint_type1 AS (
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
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        decoded_flat,
        TRY_TO_NUMBER(
            decoded_flat :epoch :: STRING
        ) AS epoch,
        TRY_TO_NUMBER(
            decoded_flat :epochLength :: STRING
        ) AS epoch_length,
        TRY_TO_NUMBER(
            decoded_flat :availableRewards :: STRING
        ) AS available_rewards_unadj,
        (available_rewards_unadj / pow(10, 18)) :: FLOAT AS available_rewards_adj,
        decoded_flat :rewards AS rewards,
        decoded_flat :serviceIds AS service_ids,
        ARRAY_SIZE(service_ids) AS num_services,
        program_name,
        _log_id,
        _inserted_timestamp
    FROM
        decoded_evt
    WHERE
        topic_0 = '0x48b735a18ed32318d316214e41387be29c52e29df4598f2b8e40fa843be3f940'
),
checkpoint_type2 AS (
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
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        decoded_flat,
        TRY_TO_NUMBER(
            decoded_flat :epoch :: STRING
        ) AS epoch,
        NULL AS epoch_length,
        TRY_TO_NUMBER(
            decoded_flat :availableRewards :: STRING
        ) AS available_rewards_unadj,
        (available_rewards_unadj / pow(10, 18)) :: FLOAT AS available_rewards_adj,
        decoded_flat :rewards AS rewards,
        decoded_flat :serviceIds AS service_ids,
        ARRAY_SIZE(service_ids) AS num_services,
        program_name,
        _log_id,
        _inserted_timestamp
    FROM
        decoded_evt
    WHERE
        topic_0 = '0x06a98bdd4732811ab3214800ed1ada2dce66a2bce301d250c3ca7d6b461ee666'
),
checkpoint_type3 AS (
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
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        decoded_flat,
        NULL AS epoch,
        NULL AS epoch_length,
        TRY_TO_NUMBER(
            decoded_flat :availableRewards :: STRING
        ) AS available_rewards_unadj,
        (available_rewards_unadj / pow(10, 18)) :: FLOAT AS available_rewards_adj,
        NULL AS rewards,
        NULL AS service_ids,
        TRY_TO_NUMBER(
            decoded_flat :numServices :: STRING
        ) AS num_services,
        program_name,
        _log_id,
        _inserted_timestamp
    FROM
        decoded_evt
    WHERE
        topic_0 = '0x21d81d5d656869e8ce3ba8d65526a2f0dbbcd3d36f5f9999eb7c84360e45eced'
),
all_checkpoints AS (
    SELECT
        *
    FROM
        checkpoint_type1
    UNION ALL
    SELECT
        *
    FROM
        checkpoint_type2
    UNION ALL
    SELECT
        *
    FROM
        checkpoint_type3
),
evt_flat AS (
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
        epoch,
        epoch_length,
        available_rewards_unadj,
        available_rewards_adj,
        rewards,
        service_ids,
        num_services,
        TRY_TO_NUMBER(
            f1.value :: STRING
        ) AS service_id,
        TRY_TO_NUMBER(
            f2.value :: STRING
        ) AS reward_unadj,
        (reward_unadj / pow(10, 18)) :: FLOAT AS reward_adj,
        program_name,
        _log_id,
        _inserted_timestamp
    FROM
        all_checkpoints,
        LATERAL FLATTEN(
            input => service_ids
        ) AS f1,
        LATERAL FLATTEN(
            input => rewards
        ) AS f2
    WHERE
        f1.index = f2.index
)
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
    epoch,
    epoch_length,
    available_rewards_unadj,
    available_rewards_adj,
    service_id,
    reward_unadj,
    reward_adj,
    num_services,
    program_name,
    rewards,
    service_ids,
    _log_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index','service_id']
    ) }} AS service_checkpoint_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    evt_flat
