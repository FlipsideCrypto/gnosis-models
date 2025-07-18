 {{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta={
        'database_tags':{
            'table': {
                'PROTOCOL': 'AAVE, AGAVE, REALT, SPARK',
                'PURPOSE': 'LENDING, FLASHLOANS'
            }
        }
    },
    tags = ['gold','defi','lending','curated','ez']
) }}

SELECT
    block_number,
    block_timestamp,
    tx_hash,
    contract_address,
    event_name,
    event_index,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    platform,
    initiator,
    target,
    protocol_market,
    flashloan_token,
    flashloan_token_symbol,
    flashloan_amount_unadj,
    flashloan_amount,
    flashloan_amount_usd,
    premium_amount_unadj,
    premium_amount,
    premium_amount_usd,
    complete_lending_flashloans_id AS ez_lending_flashloans_id,
    inserted_timestamp,
    modified_timestamp
FROM 
    {{ ref('silver__complete_lending_flashloans') }}