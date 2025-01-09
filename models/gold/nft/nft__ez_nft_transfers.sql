{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'NFT' } } }
) }}

SELECT
    block_timestamp,
    block_number,
    tx_hash,
    tx_position, -- new column
    event_index,
    intra_event_index,
    token_transfer_type, -- new column
    iff(from_address = '0x0000000000000000000000000000000000000000', true, false) AS is_mint, -- new column
    event_type, -- deprecate 
     from_address AS nft_from_address, -- deprecate
    to_address AS nft_to_address, -- deprecate
    from_address, -- new column
    to_address, -- new column
    contract_address AS nft_address,
    contract_address, -- new column
    tokenId, -- deprecate
    erc1155_value, -- deprecate 
    tokenid as token_id, -- new column
    coalesce(erc1155_value, 0) AS quantity, -- new column
    token_standard, -- new column
    project_name, -- deprecate
    name, -- new column
    origin_function_signature, -- new column
    origin_from_address, -- new column
    origin_to_address, -- new column
    COALESCE (
        nft_transfers_id,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash','event_index','intra_event_index']
        ) }}
    ) AS ez_nft_transfers_id,
    COALESCE(
        inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver__nft_transfers') }}
