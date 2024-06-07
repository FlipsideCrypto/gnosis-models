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
        TRY_TO_NUMBER(
            decoded_flat :epoch :: STRING
        ) AS epoch,
        decoded_flat :multisigs AS multisigs,
        decoded_flat :owners AS owners,
        decoded_flat :serviceIds AS service_ids,
        ARRAY_SIZE(service_ids) AS num_services,
        decoded_flat :serviceInactivity AS service_inactivity,
        CASE
            WHEN contract_address = '0xee9f19b5df06c7e8bfc7b28745dcf944c504198a' THEN 'Alpha'
            WHEN contract_address = '0x43fb32f25dce34eb76c78c7a42c8f40f84bcd237' THEN 'Coastal'
            WHEN contract_address = '0x2ef503950be67a98746f484da0bbada339df3326' THEN 'Alpine'
        END AS program_name,
        _log_id,
        _inserted_timestamp
    FROM
        {{ ref('silver__decoded_logs') }}
    WHERE
        contract_address IN (
            '0xee9f19b5df06c7e8bfc7b28745dcf944c504198a',
            -- StakingProxy (Alpha)
            '0x43fb32f25dce34eb76c78c7a42c8f40f84bcd237',
            --ServiceStakingTokenMechUsage (Coastal)
            '0x2ef503950be67a98746f484da0bbada339df3326' --ServiceStakingTokenMechUsage (Alpine)
        )
        AND topic_0 = '0xd19a3d42ed383465e4058c322d9411aeac76ddb8454d22e139fc99808bd56952' --ServicesEvicted
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
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_index,
    event_name,
    epoch,
    multisigs,
    owners,
    service_ids,
    num_services,
    service_inactivity,
    program_name,
    _log_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index']
    ) }} AS services_evicted_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    decoded_evt
