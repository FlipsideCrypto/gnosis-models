{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
    'database_tags':{
        'table': {
            'PROTOCOL': 'CURVE, BALANCER, SUSHI, HONEYSWAP, SWAPR',
            'PURPOSE': 'DEX, LIQUIDITY, POOLS, LP, SWAPS',
            }
        }
    },
    tags = ['gold','defi','dex','curated']
) }}

SELECT
    block_number AS creation_block,
    block_timestamp AS creation_time,
    tx_hash AS creation_tx,
    platform,
    contract_address AS factory_address,
    pool_address,
    pool_name,
    tokens,
    symbols,
    decimals,
    COALESCE (
        complete_dex_liquidity_pools_id,
        {{ dbt_utils.generate_surrogate_key(
            ['pool_address']
        ) }}
    ) AS dim_dex_liquidity_pools_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver_dex__complete_dex_liquidity_pools') }}