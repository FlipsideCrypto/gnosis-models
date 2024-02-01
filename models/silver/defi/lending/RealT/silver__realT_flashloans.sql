{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['reorg','curated']
) }}

WITH 
atoken_meta AS (
    SELECT
        atoken_address,
        RealT_version_pool,
        atoken_symbol,
        atoken_name,
        atoken_decimals,
        underlying_address,
        underlying_symbol,
        underlying_name,
        underlying_decimals,
        atoken_version,
        atoken_created_block,
        atoken_stable_debt_address,
        atoken_variable_debt_address
    FROM
        {{ ref('silver__realT_tokens') }}
),
flashloan AS (

    SELECT
        tx_hash,
        block_number,
        block_timestamp,
        event_index,
        origin_from_address,
        origin_to_address,
        origin_function_signature,
        contract_address,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS target_address,
        origin_to_address AS initiator_address,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS RealT_market,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS flashloan_quantity,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: INTEGER AS premium_quantity,
        utils.udf_hex_to_int(
            topics [3] :: STRING
        ) :: INTEGER AS refferalCode,
        COALESCE(
            origin_to_address,
            contract_address
        ) AS lending_pool_contract,
        'RealT V3' AS RealT_version,
        _inserted_timestamp,
        _log_id
    FROM
        {{ ref('silver__logs') }}
    WHERE
        topics [0] :: STRING IN (
            '0xc6a898309e823ee50bac64e45ca8adba6690e99e7841c45d754e2a38e9019d9b',
            '0x1e77446728e5558aa1b7e81e0cdab9cc1b075ba893b740600c76a315c2caa553',
            '0xb3d084820fb1a9decffb176436bd02558d15fac9b0ddfed8c465bc7359d7dce0'
        )

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(
            _inserted_timestamp
        ) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
{% endif %}
AND contract_address IN (SELECT distinct(RealT_version_pool) from atoken_meta)
AND tx_status = 'SUCCESS' --excludes failed txs
)
SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    RealT_market,
    atoken_meta.atoken_address AS RealT_token,
    flashloan_quantity AS flashloan_amount_unadj,
    flashloan_quantity / pow(
        10,
        atoken_meta.underlying_decimals
    ) AS flashloan_amount,
    premium_quantity AS premium_amount_unadj,
    premium_quantity / pow(
        10,
        atoken_meta.underlying_decimals
    ) AS premium_amount,
    initiator_address AS initiator_address,
    target_address AS target_address,
    RealT_version AS platform,
    atoken_meta.underlying_symbol AS symbol,
    'base' AS blockchain,
    _log_id,
    _inserted_timestamp
FROM
    flashloan
    LEFT JOIN atoken_meta
    ON flashloan.RealT_market = atoken_meta.underlying_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
