{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PROTOCOL': 'OLAS, AUTONOLAS, VALORY',
    'PURPOSE': 'AI, SERVICES, MECH' } } }
) }}

SELECT
    r.request_id,
    r.sender_address,
    r.prompt_link,
    d.delivery_link,
    {{ dbt_utils.generate_surrogate_key(
        ['r.mech_requests_id','d.mech_delivers_id']
    ) }} AS ez_mech_activity_id,
    GREATEST(
        r.inserted_timestamp,
        d.inserted_timestamp
    ) AS inserted_timestamp,
    GREATEST(
        r.modified_timestamp,
        d.modified_timestamp
    ) AS modified_timestamp
FROM
    {{ ref('silver_olas__mech_requests') }}
    r
    INNER JOIN {{ ref('silver_olas__mech_delivers') }}
    d USING(request_id)
