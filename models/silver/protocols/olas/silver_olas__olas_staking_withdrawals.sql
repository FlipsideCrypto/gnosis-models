{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['curated','reorg']
) }}

WITH withdrawals AS (

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
        'Deposit' AS event_name,
        DATA,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topic_1, 27, 40)) AS withdrawer_address,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0]
            )
        ) AS amount,
        (amount / pow(10, 18)) :: FLOAT AS amount_adj,
        CASE
            WHEN contract_address = '0xee9f19b5df06c7e8bfc7b28745dcf944c504198a' THEN 'Alpha'
            WHEN contract_address = '0x43fb32f25dce34eb76c78c7a42c8f40f84bcd237' THEN 'Coastal'
            WHEN contract_address = '0x2ef503950be67a98746f484da0bbada339df3326' THEN 'Alpine'
            WHEN contract_address = '0x5add592ce0a1b5dcecebb5dcac086cd9f9e3ea5c' THEN 'Everest'
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
            '0x2ef503950be67a98746f484da0bbada339df3326',
            --ServiceStakingTokenMechUsage (Alpine)
            '0x5add592ce0a1b5dcecebb5dcac086cd9f9e3ea5c' --ServiceStakingTokenMechUsage (Everest)
        )
        AND topic_0 = '0x884edad9ce6fa2440d8a54cc123490eb96d2768479d49ff9c7366125a9424364' --Withdraw (erc20)
        AND tx_status = 'SUCCESS'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
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
    withdrawer_address,
    amount,
    amount_adj,
    'OLAS' AS token_symbol,
    '0xce11e14225575945b8e6dc0d4f2dd4c570f79d9f' AS token_address,
    program_name,
    _log_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS olas_staking_withdrawals_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    withdrawals
