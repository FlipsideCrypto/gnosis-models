{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    A.block_number AS block_number,
    HASH AS block_hash,
    -- new column
    block_timestamp,
    'mainnet' AS network,
    tx_count,
    SIZE,
    miner,
    extra_data,
    parent_hash,
    gas_used,
    gas_limit,
    base_fee_per_gas,
    --new
    difficulty,
    total_difficulty,
    sha3_uncles,
    uncles AS uncle_blocks,
    nonce,
    --new column
    receipts_root,
    state_root,
    -- new column
    transactions_root,
    -- new column
    logs_bloom,
    -- new column
    {# withdrawals,
    -- new column
    withdrawals_root,
    -- new column #}
    COALESCE (
        blocks_id,
        {{ dbt_utils.generate_surrogate_key(
            ['a.block_number']
        ) }}
    ) AS fact_blocks_id,
    GREATEST(
        COALESCE(
            A.inserted_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            d.inserted_timestamp,
            '2000-01-01'
        )
    ) AS inserted_timestamp,
    GREATEST(
        COALESCE(
            A.modified_timestamp,
            '2000-01-01'
        ),
        COALESCE(
            d.modified_timestamp,
            '2000-01-01'
        )
    ) AS modified_timestamp,
    'gnosis' AS blockchain,
    --deprecate
    HASH,
    --deprecate
    OBJECT_CONSTRUCT(
        'baseFeePerGas',
        base_fee_per_gas,
        'difficulty',
        difficulty,
        'extraData',
        extra_data,
        'gasLimit',
        gas_limit,
        'gasUsed',
        gas_used,
        'hash',
        HASH,
        'logsBloom',
        logs_bloom,
        'miner',
        miner,
        'nonce',
        nonce,
        'number',
        NUMBER,
        'parentHash',
        parent_hash,
        'receiptsRoot',
        receipts_root,
        'sha3Uncles',
        sha3_uncles,
        'size',
        SIZE,
        'stateRoot',
        state_root,
        'timestamp',
        block_timestamp,
        'totalDifficulty',
        total_difficulty,
        'transactionsRoot',
        transactions_root,
        'uncles',
        uncles
    ) AS block_header_json --deprecate
FROM
    {{ ref('silver__blocks') }} A
    LEFT JOIN {{ ref('silver__tx_count') }}
    d
    ON A.block_number = d.block_number
