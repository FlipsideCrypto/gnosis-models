{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH stake AS (

    SELECT
        block_number,
        block_timestamp,
        tx_hash,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        event_index,
        topics [0] :: STRING AS topic_0,
        topics [1] :: STRING AS topic_1,
        topics [2] :: STRING AS topic_2,
        topics [3] :: STRING AS topic_3,
        'ServiceStaked' AS event_name,
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
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
        ARRAY_CONSTRUCT(
            TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [3] :: STRING)),
            TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [4] :: STRING))
        ) AS nonces,
        CASE
            WHEN contract_address = '0xee9f19b5df06c7e8bfc7b28745dcf944c504198a' THEN 'Alpha'
            WHEN contract_address = '0x43fb32f25dce34eb76c78c7a42c8f40f84bcd237' THEN 'Coastal'
            WHEN contract_address = '0x2ef503950be67a98746f484da0bbada339df3326' THEN 'Alpine'
        END AS program_name,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address IN (
            '0xee9f19b5df06c7e8bfc7b28745dcf944c504198a',
            --StakingProxy (Alpha)
            '0x43fb32f25dce34eb76c78c7a42c8f40f84bcd237',
            --ServiceStakingTokenMechUsage (Coastal)
            '0x2ef503950be67a98746f484da0bbada339df3326' --ServiceStakingTokenMechUsage (Alpine)
        )
        AND topic_0 = '0xaa6b005b4958114a0c90492461c24af6525ae0178db7fbf44125ae9217c69ccb' --ServiceStaked
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
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
        topics [0] :: STRING AS topic_0,
        topics [1] :: STRING AS topic_1,
        topics [2] :: STRING AS topic_2,
        topics [3] :: STRING AS topic_3,
        'ServiceStaked' AS event_name,
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                topic_1
            )
        ) AS service_id,
        CONCAT('0x', SUBSTR(topic_2, 27, 40)) AS owner_address,
        CONCAT('0x', SUBSTR(topic_3, 27, 40)) AS multisig_address,
        NULL AS epoch,
        ARRAY_CONSTRUCT(
            TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [0] :: STRING)),
            TRY_TO_NUMBER(utils.udf_hex_to_int(segmented_data [1] :: STRING))
        ) AS nonces,
        'Everest' AS program_name,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__logs') }}
    WHERE
        contract_address = '0x5add592ce0a1b5dcecebb5dcac086cd9f9e3ea5c' --ServiceStakingTokenMechUsage (Everest)
        AND topic_0 = '0x5d43ac9b1b213902df90d405b0006308578486b6c62182c5df202ed572c844e4' --ServiceStaked
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
),
all_staking AS (
    SELECT
        *
    FROM
        stake
    UNION ALL
    SELECT
        *
    FROM
        stake_everest
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
    service_id,
    owner_address,
    multisig_address,
    epoch,
    nonces,
    program_name,
    _log_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS services_staked_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    all_staking
