{{ config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key = "block_number",
    cluster_by = ['block_timestamp::DATE'],
    tags = ['silver','defi','lending','curated']
) }}

WITH atoken_meta AS (

    SELECT
        atoken_address,
        version_pool,
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
        {{ ref('silver__spark_tokens') }}
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
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS asset_1,
        utils.udf_hex_to_int(
            segmented_data [1] :: STRING
        ) :: INTEGER AS flashloan_quantity,
        utils.udf_hex_to_int(
            segmented_data [3] :: STRING
        ) :: INTEGER AS premium_quantity,
        utils.udf_hex_to_int(
            topics [3] :: STRING
        ) :: INTEGER AS refferalCode,
        CONCAT(
            tx_hash :: STRING,
            '-',
            event_index :: STRING
        ) AS _log_id,
        modified_timestamp AS _inserted_timestamp,
        COALESCE(
            origin_to_address,
            contract_address
        ) AS lending_pool_contract,
        'Spark' AS spark_version,
        CASE
            WHEN asset_1 = '0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee' THEN '0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2'
            ELSE asset_1
        END AS spark_market
    FROM
        {{ ref('core__fact_event_logs') }}
    WHERE
        topics [0] :: STRING = '0xefefaba5e921573100900a3ad9cf29f222d995fb3b6045797eaea7521bd8d6f0'

{% if is_incremental() %}
AND _inserted_timestamp >= (
    SELECT
        MAX(_inserted_timestamp) - INTERVAL '12 hours'
    FROM
        {{ this }}
)
AND _inserted_timestamp >= SYSDATE() - INTERVAL '7 day'
{% endif %}
AND contract_address IN (
    SELECT
        DISTINCT(version_pool)
    FROM
        atoken_meta
)
AND tx_succeeded
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
    LOWER(
        spark_market
    ) AS spark_market,
    LOWER(
        atoken_meta.atoken_address
    ) AS spark_token,
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
    LOWER(initiator_address) AS initiator_address,
    LOWER(target_address) AS target_address,
    spark_version AS platform,
    atoken_meta.underlying_symbol AS symbol,
    'ethereum' AS blockchain,
    _log_id,
    _inserted_timestamp
FROM
    flashloan
    LEFT JOIN atoken_meta
    ON flashloan.spark_market = atoken_meta.underlying_address qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
    _inserted_timestamp DESC)) = 1
