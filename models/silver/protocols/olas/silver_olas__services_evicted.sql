{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_olas','curated','olas']
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
        topic_0,
        topic_1,
        topic_2,
        topic_3,
        decoded_log,
        TRY_TO_NUMBER(
            decoded_log :epoch :: STRING
        ) AS epoch,
        decoded_log :multisigs AS multisigs,
        decoded_log :owners AS owners,
        decoded_log :serviceIds AS service_ids,
        ARRAY_SIZE(service_ids) AS num_services,
        decoded_log :serviceInactivity AS service_inactivities,
        CASE
            WHEN contract_address = '0xee9f19b5df06c7e8bfc7b28745dcf944c504198a' THEN 'Alpha'
            WHEN contract_address = '0x43fb32f25dce34eb76c78c7a42c8f40f84bcd237' THEN 'Coastal'
            WHEN contract_address = '0x2ef503950be67a98746f484da0bbada339df3326' THEN 'Alpine'
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
        {{ ref('core__ez_decoded_event_logs') }}
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
        AND topic_0 = '0xd19a3d42ed383465e4058c322d9411aeac76ddb8454d22e139fc99808bd56952' --ServicesEvicted
        AND tx_succeeded

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND block_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
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
        multisigs,
        owners,
        service_ids,
        num_services,
        service_inactivities,
        epoch,
        TRY_TO_NUMBER(
            f1.value :: STRING
        ) AS service_id,
        LOWER(
            f2.value :: STRING
        ) AS multisig_address,
        LOWER(
            f3.value :: STRING
        ) AS owner_address,
        TRY_TO_NUMBER(
            f4.value :: STRING
        ) AS service_inactivity,
        program_name,
        _log_id,
        _inserted_timestamp
    FROM
        decoded_evt,
        LATERAL FLATTEN(
            input => service_ids
        ) AS f1,
        LATERAL FLATTEN(
            input => multisigs
        ) AS f2,
        LATERAL FLATTEN(
            input => owners
        ) AS f3,
        LATERAL FLATTEN(
            input => service_inactivities
        ) AS f4
    WHERE
        f1.index = f2.index
        AND f2.index = f3.index
        AND f3.index = f4.index
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
    service_id,
    owner_address,
    multisig_address,
    service_inactivity,
    program_name,
    num_services,
    multisigs,
    owners,
    service_ids,
    service_inactivities,
    _log_id,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash','event_index','service_id']
    ) }} AS services_evicted_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    evt_flat
