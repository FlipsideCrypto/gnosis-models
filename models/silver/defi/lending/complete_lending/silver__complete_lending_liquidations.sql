-- depends_on: {{ ref('silver__complete_token_prices') }}
{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform'],
  cluster_by = ['block_timestamp::DATE','platform'],
  post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(tx_hash, origin_from_address, origin_to_address, origin_function_signature, contract_address, event_name, liquidator, borrower, collateral_token, collateral_token_symbol, debt_token, debt_token_symbol, protocol_market), SUBSTRING(origin_function_signature, event_name, liquidator, borrower, collateral_token, collateral_token_symbol, debt_token, debt_token_symbol, protocol_market)",
  tags = ['silver','defi','lending','curated','heal']
) }}

WITH aave AS (

  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    liquidator,
    borrower,
    amount_unadj,
    amount AS liquidated_amount,
    NULL AS liquidated_amount_usd,
    collateral_aave_token AS protocol_collateral_asset,
    collateral_asset,
    collateral_token_symbol AS collateral_asset_symbol,
    debt_asset,
    debt_token_symbol AS debt_asset_symbol,
    'Aave V3' AS platform,
    'gnosis' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__aave_liquidations') }}

{% if is_incremental() and 'aave' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
spark AS (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    liquidator,
    borrower,
    amount_unadj,
    amount AS liquidated_amount,
    NULL AS liquidated_amount_usd,
    collateral_spark_token AS protocol_collateral_asset,
    collateral_asset,
    collateral_token_symbol AS collateral_asset_symbol,
    debt_asset,
    debt_token_symbol AS debt_asset_symbol,
    'Spark' AS platform,
    'gnosis' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__spark_liquidations') }}

{% if is_incremental() and 'spark' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
agave AS (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    liquidator,
    borrower,
    amount_unadj,
    amount AS liquidated_amount,
    NULL AS liquidated_amount_usd,
    collateral_agave_token AS protocol_collateral_asset,
    collateral_asset,
    collateral_token_symbol AS collateral_asset_symbol,
    debt_asset,
    debt_token_symbol AS debt_asset_symbol,
    'Agave' AS platform,
    'gnosis' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__agave_liquidations') }}

{% if is_incremental() and 'agave' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
realt AS (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    contract_address,
    liquidator,
    borrower,
    amount_unadj,
    amount AS liquidated_amount,
    NULL AS liquidated_amount_usd,
    collateral_realt_token AS protocol_collateral_asset,
    collateral_asset,
    collateral_token_symbol AS collateral_asset_symbol,
    debt_asset,
    debt_token_symbol AS debt_asset_symbol,
    'realt' AS platform,
    'gnosis' AS blockchain,
    _LOG_ID,
    _INSERTED_TIMESTAMP
  FROM
    {{ ref('silver__realt_liquidations') }}

{% if is_incremental() and 'realt' not in var('HEAL_MODELS') %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
    FROM
      {{ this }}
  )
{% endif %}
),
liquidation_union AS (
  SELECT
    *
  FROM
    aave
  UNION ALL
  SELECT
    *
  FROM
    agave
  UNION ALL
  SELECT
    *
  FROM
    spark
  UNION ALL
  SELECT
    *
  FROM
    realt
),
complete_lending_liquidations AS (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    A.contract_address,
    'LiquidationCall' AS event_name,
    liquidator,
    borrower,
    protocol_collateral_asset AS protocol_market,
    collateral_asset AS collateral_token,
    collateral_asset_symbol AS collateral_token_symbol,
    amount_unadj,
    liquidated_amount AS amount,
    ROUND(
      liquidated_amount * p.price,
      2
    ) AS amount_usd,
    debt_asset AS debt_token,
    debt_asset_symbol AS debt_token_symbol,
    platform,
    A.blockchain,
    A._LOG_ID,
    A._INSERTED_TIMESTAMP
  FROM
    liquidation_union A
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p
    ON collateral_asset = p.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p.hour
),

{% if is_incremental() and var(
  'HEAL_MODEL'
) %}
heal_model AS (
  SELECT
    tx_hash,
    block_number,
    block_timestamp,
    event_index,
    origin_from_address,
    origin_to_address,
    origin_function_signature,
    t0.contract_address,
    event_name,
    liquidator,
    borrower,
    protocol_market,
    collateral_token,
    collateral_token_symbol,
    amount_unadj,
    amount,
    ROUND(
      amount * p.price,
      2
    ) AS amount_usd_heal,
    debt_token,
    debt_token_symbol,
    platform,
    t0.blockchain,
    t0._LOG_ID,
    t0._INSERTED_TIMESTAMP
  FROM
    {{ this }}
    t0
    LEFT JOIN {{ ref('price__ez_prices_hourly') }}
    p
    ON t0.collateral_token = p.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p.hour
  WHERE
    CONCAT(
      t0.block_number,
      '-',
      t0.platform
    ) IN (
      SELECT
        CONCAT(
          t1.block_number,
          '-',
          t1.platform
        )
      FROM
        {{ this }}
        t1
      WHERE
        t1.amount_usd IS NULL
        AND t1._inserted_timestamp < (
          SELECT
            MAX(
              _inserted_timestamp
            ) - INTERVAL '{{ var("LOOKBACK", "4 hours") }}'
          FROM
            {{ this }}
        )
        AND EXISTS (
          SELECT
            1
          FROM
            {{ ref('silver__complete_token_prices') }}
            p
          WHERE
            p._inserted_timestamp > DATEADD('DAY', -14, SYSDATE())
            AND p.price IS NOT NULL
            AND p.token_address = t1.collateral_token
            AND p.hour = DATE_TRUNC(
              'hour',
              t1.block_timestamp
            )
        )
      GROUP BY
        1
    )
),
{% endif %}

FINAL AS (
  SELECT
    *
  FROM
    complete_lending_liquidations

{% if is_incremental() and var(
  'HEAL_MODEL'
) %}
UNION ALL
SELECT
  tx_hash,
  block_number,
  block_timestamp,
  event_index,
  origin_from_address,
  origin_to_address,
  origin_function_signature,
  contract_address,
  event_name,
  liquidator,
  borrower,
  protocol_market,
  collateral_token,
  collateral_token_symbol,
  amount_unadj,
  amount,
  amount_usd_heal AS amount_usd,
  debt_token,
  debt_token_symbol,
  platform,
  blockchain,
  _LOG_ID,
  _INSERTED_TIMESTAMP
FROM
  heal_model
{% endif %}
)
SELECT
  *,
  {{ dbt_utils.generate_surrogate_key(
    ['tx_hash','event_index']
  ) }} AS complete_lending_liquidations_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL qualify(ROW_NUMBER() over(PARTITION BY _log_id
ORDER BY
  _inserted_timestamp DESC)) = 1
