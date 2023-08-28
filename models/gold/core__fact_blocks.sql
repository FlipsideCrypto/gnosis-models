{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true }
) }}

SELECT
    A.block_number AS block_number,
    block_timestamp,
    'mainnet' AS network,
    'gnosis' AS blockchain,
    tx_count,
    difficulty,
    total_difficulty,
    miner,
    extra_data,
    gas_limit,
    gas_used,
    HASH,
    parent_hash,
    receipts_root,
    sha3_uncles,
    SIZE,
    uncles AS uncle_blocks,
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
    ) AS block_header_json
FROM
    {{ ref('silver__blocks') }} A
    LEFT JOIN {{ ref('silver__tx_count') }}
    d
    ON A.block_number = d.block_number
