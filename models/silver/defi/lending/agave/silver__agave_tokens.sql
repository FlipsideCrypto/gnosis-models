{{ config(
    materialized = 'incremental',
    tags = ['curated']
) }}

WITH contracts as (
    SELECT
        *
    FROM
        {{ ref('silver__contracts') }}
),

agave_token_pull AS (
    SELECT
        block_number AS atoken_created_block,
        origin_from_address as token_creator_address,
        lower('0x5E15d5E33d318dCEd84Bfe3F4EACe07909bE6d9c') as version_pool,
        C.token_symbol AS a_token_symbol,
        regexp_substr_all(SUBSTR(DATA, 3, len(DATA)), '.{64}') AS segmented_data,
        CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) AS a_token_address,
        CONCAT('0x', SUBSTR(segmented_data [0] :: STRING, 27, 40)) :: STRING AS atoken_stable_debt_address,
        CONCAT('0x', SUBSTR(segmented_data [1] :: STRING, 27, 40)) :: STRING AS atoken_variable_debt_address,
        C.token_decimals AS a_token_decimals,
        'Agave' AS agave_version,
        C.token_name AS a_token_name,
        c2.token_symbol AS underlying_symbol,
        CONCAT('0x', SUBSTR(topics [1] :: STRING, 27, 40)) AS underlying_address,
        c2.token_name AS underlying_name,
        c2.token_decimals AS underlying_decimals,
        l._inserted_timestamp,
        l._log_id
    FROM
        {{ ref('silver__logs') }}
        l
        LEFT JOIN contracts C
        ON a_token_address = C.contract_address
        LEFT JOIN contracts c2
        ON underlying_address = c2.contract_address
    WHERE
        topics [0] = '0x3a0ca721fc364424566385a1aa271ed508cc2c0949c2272575fb3013a163a45f'
        AND (
            a_token_name LIKE '%Agave%'
            OR c2.token_symbol = 'GHO'
        )

    {% if is_incremental() %}
    AND l._inserted_timestamp >= (
        SELECT
            MAX(
                _inserted_timestamp
            ) - INTERVAL '12 hours'
        FROM
            {{ this }}
    )
    AND CONCAT('0x', SUBSTR(topics [2] :: STRING, 27, 40)) NOT IN (
    SELECT
        atoken_address
    FROM
        {{ this }}
)
    {% endif %}
),
agave_token_pull_2 AS (
    SELECT
        atoken_created_block,
        version_pool,
        a_token_symbol,
        a_token_address,
        atoken_stable_debt_address,
        atoken_variable_debt_address,
        a_token_decimals,
        agave_version,
        a_token_name,
        underlying_symbol,
        underlying_name,
        underlying_decimals,
        underlying_address,
        _inserted_timestamp,
        _log_id
    FROM
        agave_token_pull
),

agave_backfill_1 as (
    SELECT
        atoken_created_block,
        version_pool,
        a_token_symbol AS atoken_symbol,
        a_token_address AS atoken_address,
        atoken_stable_debt_address,
        atoken_variable_debt_address,
        a_token_decimals AS atoken_decimals,
        agave_version AS atoken_version,
        a_token_name AS atoken_name,
        underlying_symbol,
        underlying_address,
        underlying_decimals,
        underlying_name,
        _inserted_timestamp,
        _log_id
    FROM
        agave_token_pull_2
)
SELECT
    atoken_created_block,
    version_pool,
    atoken_symbol,
    atoken_address,
    atoken_stable_debt_address,
    atoken_variable_debt_address,
    atoken_decimals,
    atoken_version,
    atoken_name,
    underlying_symbol,
    underlying_address,
    underlying_decimals,
    underlying_name,
    _inserted_timestamp,
    _log_id
FROM
    agave_backfill_1