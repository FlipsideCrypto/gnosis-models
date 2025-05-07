{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_olas','curated','olas']
) }}

WITH all_evt AS (

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
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CASE
            WHEN contract_address = '0xee9f19b5df06c7e8bfc7b28745dcf944c504198a' THEN 'Alpha'
            WHEN contract_address = '0x43fb32f25dce34eb76c78c7a42c8f40f84bcd237' THEN 'Coastal'
            WHEN contract_address = '0x2ef503950be67a98746f484da0bbada339df3326' THEN 'Alpine'
            WHEN contract_address = '0x5add592ce0a1b5dcecebb5dcac086cd9f9e3ea5c' THEN 'Everest'
            WHEN contract_address = '0x5344b7dd311e5d3dddd46a4f71481bd7b05aaa3e' THEN 'Quickstart Beta - Expert'
            WHEN contract_address = '0x389b46c259631acd6a69bde8b6cee218230bae8c' THEN 'Quickstart Beta - Hobbyist'
            WHEN contract_address = '0xef44fb0842ddef59d37f85d61a1ef492bba6135d' THEN 'Pearl Beta'
        END AS program_name,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp
    FROM
        {{ ref('core__fact_event_logs') }}
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
            '0xaa6b005b4958114a0c90492461c24af6525ae0178db7fbf44125ae9217c69ccb',
            --ServiceStaked
            '0x950733f4c0bf951b8e770f3cc619a4288e7b59b1236d59aeaf2c238488e8ae81',
            --ServiceUnstaked
            '0x5d43ac9b1b213902df90d405b0006308578486b6c62182c5df202ed572c844e4',
            --ServiceStaked (Everest)
            '0x246ee6115bfd84e00097b16569c2ff2f822026bb9595a82cd2c1e69d4b6ea50c',
            --ServiceUnstaked (Everest)
            '0x6d789d063e079a4c156e77a20008529fc448dca2cd7e5e7a20abf969fffb9226' --ServiceUnstaked (Beta)
        )
        AND tx_succeeded

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
stake AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        'ServiceStaked' AS event_name,
        segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                topic_1
            )
        ) AS service_id,
        CONCAT('0x', SUBSTR(topic_2, 27, 40)) AS owner_address,
        CONCAT('0x', SUBSTR(topic_3, 27, 40)) AS multisig_address,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS epoch,
        NULL AS reward_unadj,
        NULL AS available_rewards_unadj,
        NULL AS ts_start,
        ARRAY_CONSTRUCT(
            TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [3] :: STRING)),
            TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [4] :: STRING))
        ) AS nonces,
        program_name,
        _log_id,
        _inserted_timestamp
    FROM
        all_evt
    WHERE
        contract_address IN (
            '0xee9f19b5df06c7e8bfc7b28745dcf944c504198a',
            --StakingProxy (Alpha)
            '0x43fb32f25dce34eb76c78c7a42c8f40f84bcd237',
            --ServiceStakingTokenMechUsage (Coastal)
            '0x2ef503950be67a98746f484da0bbada339df3326',
            --ServiceStakingTokenMechUsage (Alpine)
            '0x5344b7dd311e5d3dddd46a4f71481bd7b05aaa3e',
            --Quickstart Beta - Expert
            '0x389b46c259631acd6a69bde8b6cee218230bae8c',
            --Quickstart Beta - Hobbyist
            '0xef44fb0842ddef59d37f85d61a1ef492bba6135d' --Pearl Beta
        )
        AND topic_0 = '0xaa6b005b4958114a0c90492461c24af6525ae0178db7fbf44125ae9217c69ccb' --ServiceStaked
),
stake_everest AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        'ServiceStaked' AS event_name,
        segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                topic_1
            )
        ) AS service_id,
        CONCAT('0x', SUBSTR(topic_2, 27, 40)) AS owner_address,
        CONCAT('0x', SUBSTR(topic_3, 27, 40)) AS multisig_address,
        NULL AS epoch,
        NULL AS reward_unadj,
        NULL AS available_rewards_unadj,
        NULL AS ts_start,
        ARRAY_CONSTRUCT(
            TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [0] :: STRING)),
            TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [1] :: STRING))
        ) AS nonces,
        program_name,
        _log_id,
        _inserted_timestamp
    FROM
        all_evt
    WHERE
        contract_address = '0x5add592ce0a1b5dcecebb5dcac086cd9f9e3ea5c' --ServiceStakingTokenMechUsage (Everest)
        AND topic_0 = '0x5d43ac9b1b213902df90d405b0006308578486b6c62182c5df202ed572c844e4' --ServiceStaked
),
unstake AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        'ServiceUnstaked' AS event_name,
        segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                topic_1
            )
        ) AS service_id,
        CONCAT('0x', SUBSTR(topic_2, 27, 40)) AS owner_address,
        CONCAT('0x', SUBSTR(topic_3, 27, 40)) AS multisig_address,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS epoch,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) AS reward_unadj,
        NULL AS available_rewards_unadj,
        NULL AS ts_start,
        ARRAY_CONSTRUCT(
            TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [4] :: STRING)),
            TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [5] :: STRING))
        ) AS nonces,
        program_name,
        _log_id,
        _inserted_timestamp
    FROM
        all_evt
    WHERE
        contract_address IN (
            '0xee9f19b5df06c7e8bfc7b28745dcf944c504198a',
            --StakingProxy (Alpha)
            '0x43fb32f25dce34eb76c78c7a42c8f40f84bcd237',
            --ServiceStakingTokenMechUsage (Coastal)
            '0x2ef503950be67a98746f484da0bbada339df3326' --ServiceStakingTokenMechUsage (Alpine)
        )
        AND topic_0 = '0x950733f4c0bf951b8e770f3cc619a4288e7b59b1236d59aeaf2c238488e8ae81' --ServiceUnstaked
),
unstake_everest AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        'ServiceUnstaked' AS event_name,
        segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                topic_1
            )
        ) AS service_id,
        CONCAT('0x', SUBSTR(topic_2, 27, 40)) AS owner_address,
        CONCAT('0x', SUBSTR(topic_3, 27, 40)) AS multisig_address,
        NULL AS epoch,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) AS reward_unadj,
        NULL AS available_rewards_unadj,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) AS ts_start,
        ARRAY_CONSTRUCT(
            TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [4] :: STRING)),
            TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [5] :: STRING))
        ) AS nonces,
        program_name,
        _log_id,
        _inserted_timestamp
    FROM
        all_evt
    WHERE
        contract_address = '0x5add592ce0a1b5dcecebb5dcac086cd9f9e3ea5c' --ServiceStakingTokenMechUsage (Everest)
        AND topic_0 = '0x246ee6115bfd84e00097b16569c2ff2f822026bb9595a82cd2c1e69d4b6ea50c' --ServiceUnstaked
),
unstake_beta AS (
    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        'ServiceUnstaked' AS event_name,
        segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                topic_1
            )
        ) AS service_id,
        CONCAT('0x', SUBSTR(topic_2, 27, 40)) AS owner_address,
        CONCAT('0x', SUBSTR(topic_3, 27, 40)) AS multisig_address,
        utils.udf_hex_to_int(
            segmented_data [0] :: STRING
        ) AS epoch,
        utils.udf_hex_to_int(
            segmented_data [2] :: STRING
        ) AS reward_unadj,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) AS available_rewards_unadj,
        NULL AS ts_start,
        ARRAY_CONSTRUCT(
            TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [5] :: STRING)),
            TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [6] :: STRING))
        ) AS nonces,
        program_name,
        _log_id,
        _inserted_timestamp
    FROM
        all_evt
    WHERE
        contract_address IN (
            '0x5344b7dd311e5d3dddd46a4f71481bd7b05aaa3e',
            --Quickstart Beta - Expert
            '0x389b46c259631acd6a69bde8b6cee218230bae8c',
            --Quickstart Beta - Hobbyist
            '0xef44fb0842ddef59d37f85d61a1ef492bba6135d' --Pearl Beta
        )
        AND topic_0 = '0x6d789d063e079a4c156e77a20008529fc448dca2cd7e5e7a20abf969fffb9226' --ServiceUnstaked
),
union_evt AS (
    SELECT
        *
    FROM
        stake
    UNION ALL
    SELECT
        *
    FROM
        stake_everest
    UNION ALL
    SELECT
        *
    FROM
        unstake
    UNION ALL
    SELECT
        *
    FROM
        unstake_everest
    UNION ALL
    SELECT
        *
    FROM
        unstake_beta
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
    segmented_data,
    service_id,
    owner_address,
    multisig_address,
    epoch,
    reward_unadj,
    (reward_unadj / pow(10, 18)) :: FLOAT AS reward_adj,
    available_rewards_unadj,
    (available_rewards_unadj / pow(10, 18)) :: FLOAT AS available_rewards_adj,
    ts_start,
    nonces,
    program_name,
    _log_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS service_staking_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    union_evt
