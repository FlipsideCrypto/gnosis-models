{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = 'block_number',
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver_dex','defi','dex','curated']
) }}

WITH pools AS (

    SELECT
        pool_name,
        pool_address
    FROM
        {{ ref('silver_dex__balancer_pools') }}
),
swaps_base AS (
    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        contract_address,
        'Swap' AS event_name,
        event_index,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [0] :: STRING
            )
        ) AS amount_in_unadj,
        TRY_TO_NUMBER(
            utils.udf_hex_to_int(
                segmented_data [1] :: STRING
            )
        ) AS amount_out_unadj,
        topics [1] :: STRING AS pool_id,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS token_in,
        CONCAT('0x', SUBSTR(topics [3] :: STRING, 27, 40)) AS token_out,
        SUBSTR(
            topics [1] :: STRING,
            1,
            42
        ) AS pool_address,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp,
        'balancer' AS platform,
        origin_from_address AS sender,
        origin_from_address AS tx_to
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0x2170c741c41531aec20e7c107c24eecfdd15e69c9bb0a8dd37b1840b9e0b207b'
        AND contract_address = '0xba12222222228d8ba445958a75a0704d566bf2c8'
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
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    event_index,
    amount_in_unadj,
    amount_out_unadj,
    token_in,
    token_out,
    sender,
    tx_to,
    pool_id,
    s.pool_address AS contract_address,
    pool_name,
    event_name,
    platform,
    _log_id,
    _inserted_timestamp
FROM
    swaps_base s
    INNER JOIN pools p
    ON p.pool_address = s.pool_address
