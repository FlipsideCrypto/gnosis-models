{{ config(
  materialized = 'incremental',
  incremental_strategy = 'delete+insert',
  unique_key = ['block_number','platform','version'],
  cluster_by = ['block_timestamp::DATE'],
  tags = ['curated','reorg']
) }}

WITH contracts AS (

  SELECT
    contract_address AS address,
    token_symbol AS symbol,
    token_name AS NAME,
    token_decimals AS decimals
  FROM
    {{ ref('silver__contracts') }}
),
prices AS (
  SELECT
    HOUR,
    token_address,
    price
  FROM
    {{ ref('price__ez_hourly_token_prices') }}
  WHERE
    token_address IN (
      SELECT
        DISTINCT address
      FROM
        contracts
    )
),
sushi_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
      WHEN decimals_in IS NULL THEN amount_in_unadj
      ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
      WHEN decimals_out IS NULL THEN amount_out_unadj
      ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v1' AS version,
    token_in,
    token_out,
    NULL AS pool_name,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__sushi_swaps') }}
    s
    LEFT JOIN contracts c1
    ON s.token_in = c1.address
    LEFT JOIN contracts c2
    ON s.token_out = c2.address

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
),
curve_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    s.tokens_sold AS amount_in_unadj,
    s.tokens_bought AS amount_out_unadj,
    sender,
    tx_to,
    event_index,
    platform,
    'v1' AS version,
    token_in,
    token_out,
    COALESCE(
      c1.symbol,
      s.symbol_in
    ) AS token_symbol_in,
    COALESCE(
      c2.symbol,
      s.symbol_out
    ) AS token_symbol_out,
    NULL AS pool_name,
    c1.decimals AS decimals_in,
    CASE
      WHEN decimals_in IS NOT NULL THEN s.tokens_sold / pow(
        10,
        decimals_in
      )
      ELSE s.tokens_sold
    END AS amount_in,
    c2.decimals AS decimals_out,
    CASE
      WHEN decimals_out IS NOT NULL THEN s.tokens_bought / pow(
        10,
        decimals_out
      )
      ELSE s.tokens_bought
    END AS amount_out,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__curve_swaps') }}
    s
    LEFT JOIN contracts c1
    ON c1.address = s.token_in
    LEFT JOIN contracts c2
    ON c2.address = s.token_out
  WHERE
    amount_out <> 0
    AND COALESCE(
      token_symbol_in,
      'null'
    ) <> COALESCE(token_symbol_out, 'null')

{% if is_incremental() %}
AND _inserted_timestamp >= (
  SELECT
    MAX(_inserted_timestamp) - INTERVAL '36 hours'
  FROM
    {{ this }}
)
{% endif %}
),
balancer_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    NULL AS pool_name,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    amount_out_unadj,
    CASE
      WHEN decimals_in IS NULL THEN amount_in_unadj
      ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    CASE
      WHEN decimals_out IS NULL THEN amount_out_unadj
      ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v1' AS version,
    token_in,
    token_out,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__balancer_swaps') }}
    s
    LEFT JOIN contracts c1
    ON s.token_in = c1.address
    LEFT JOIN contracts c2
    ON s.token_out = c2.address

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
),
honeyswap_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
      WHEN decimals_in IS NULL THEN amount_in_unadj
      ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
      WHEN decimals_out IS NULL THEN amount_out_unadj
      ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v1' AS version,
    token_in,
    token_out,
    NULL AS pool_name,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__honeyswap_swaps') }}
    s
    LEFT JOIN contracts c1
    ON s.token_in = c1.address
    LEFT JOIN contracts c2
    ON s.token_out = c2.address

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
),
swapr_swaps AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    event_name,
    c1.decimals AS decimals_in,
    c1.symbol AS symbol_in,
    amount_in_unadj,
    CASE
      WHEN decimals_in IS NULL THEN amount_in_unadj
      ELSE (amount_in_unadj / pow(10, decimals_in))
    END AS amount_in,
    c2.decimals AS decimals_out,
    c2.symbol AS symbol_out,
    amount_out_unadj,
    CASE
      WHEN decimals_out IS NULL THEN amount_out_unadj
      ELSE (amount_out_unadj / pow(10, decimals_out))
    END AS amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    'v1' AS version,
    token_in,
    token_out,
    NULL AS pool_name,
    _log_id,
    _inserted_timestamp
  FROM
    {{ ref('silver_dex__swapr_swaps') }}
    s
    LEFT JOIN contracts c1
    ON s.token_in = c1.address
    LEFT JOIN contracts c2
    ON s.token_out = c2.address

{% if is_incremental() %}
WHERE
  _inserted_timestamp >= (
    SELECT
      MAX(_inserted_timestamp) - INTERVAL '36 hours'
    FROM
      {{ this }}
  )
{% endif %}
),
--union all standard dex CTEs here (excludes amount_usd)
all_dex_standard AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    amount_in,
    amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    version,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    decimals_in,
    decimals_out,
    _log_id,
    _inserted_timestamp
  FROM
    balancer_swaps
  UNION ALL
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    amount_in,
    amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    version,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    decimals_in,
    decimals_out,
    _log_id,
    _inserted_timestamp
  FROM
    sushi_swaps
  UNION ALL
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    amount_in,
    amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    version,
    token_in,
    token_out,
    token_symbol_in AS symbol_in,
    token_symbol_out AS symbol_out,
    decimals_in,
    decimals_out,
    _log_id,
    _inserted_timestamp
  FROM
    curve_swaps
  UNION ALL
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    amount_in,
    amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    version,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    decimals_in,
    decimals_out,
    _log_id,
    _inserted_timestamp
  FROM
    honeyswap_swaps
  UNION ALL
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in_unadj,
    amount_out_unadj,
    amount_in,
    amount_out,
    sender,
    tx_to,
    event_index,
    platform,
    version,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    decimals_in,
    decimals_out,
    _log_id,
    _inserted_timestamp
  FROM
    swapr_swaps
),
FINAL AS (
  SELECT
    block_number,
    block_timestamp,
    tx_hash,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    contract_address,
    pool_name,
    event_name,
    amount_in_unadj,
    amount_in,
    CASE
      WHEN s.decimals_in IS NOT NULL THEN ROUND(
        amount_in * p1.price,
        2
      )
      ELSE NULL
    END AS amount_in_usd,
    amount_out_unadj,
    amount_out,
    CASE
      WHEN s.decimals_out IS NOT NULL THEN ROUND(
        amount_out * p2.price,
        2
      )
      ELSE NULL
    END AS amount_out_usd,
    sender,
    tx_to,
    event_index,
    platform,
    version,
    token_in,
    token_out,
    symbol_in,
    symbol_out,
    _log_id,
    _inserted_timestamp
  FROM
    all_dex_standard s
    LEFT JOIN prices p1
    ON s.token_in = p1.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p1.hour
    LEFT JOIN prices p2
    ON s.token_out = p2.token_address
    AND DATE_TRUNC(
      'hour',
      block_timestamp
    ) = p2.hour
)
SELECT
  f.block_number,
  f.block_timestamp,
  f.tx_hash,
  origin_function_signature,
  origin_from_address,
  origin_to_address,
  f.contract_address,
  CASE
    WHEN f.pool_name IS NULL THEN p.pool_name
    ELSE f.pool_name
  END AS pool_name,
  event_name,
  amount_in_unadj,
  amount_in,
  CASE
    WHEN amount_out_usd IS NULL
    OR ABS((amount_in_usd - amount_out_usd) / NULLIF(amount_out_usd, 0)) > 0.75
    OR ABS((amount_in_usd - amount_out_usd) / NULLIF(amount_in_usd, 0)) > 0.75 THEN NULL
    ELSE amount_in_usd
  END AS amount_in_usd,
  amount_out_unadj,
  amount_out,
  CASE
    WHEN amount_in_usd IS NULL
    OR ABS((amount_out_usd - amount_in_usd) / NULLIF(amount_in_usd, 0)) > 0.75
    OR ABS((amount_out_usd - amount_in_usd) / NULLIF(amount_out_usd, 0)) > 0.75 THEN NULL
    ELSE amount_out_usd
  END AS amount_out_usd,
  sender,
  tx_to,
  event_index,
  f.platform,
  f.version,
  token_in,
  token_out,
  symbol_in,
  symbol_out,
  f._log_id,
  f._inserted_timestamp,
  {{ dbt_utils.generate_surrogate_key(
    ['f.tx_hash','f.event_index']
  ) }} AS complete_dex_swaps_id,
  SYSDATE() AS inserted_timestamp,
  SYSDATE() AS modified_timestamp,
  '{{ invocation_id }}' AS _invocation_id
FROM
  FINAL f
  LEFT JOIN {{ ref('silver_dex__complete_dex_liquidity_pools') }}
  p
  ON f.contract_address = p.pool_address
