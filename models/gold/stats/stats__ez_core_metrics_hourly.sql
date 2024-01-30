{{ config(
    materialized = 'view',
    persist_docs ={ "relation": true,
    "columns": true },
    meta ={ 'database_tags':{ 'table':{ 'PURPOSE': 'STATS, METRICS, CORE, HOURLY',
    } } }
) }}

SELECT
    block_timestamp_hour,
    block_number_min,
    block_number_max,
    block_count,
    transaction_count,
    transaction_count_success,
    transaction_count_failed,
    unique_from_count,
    unique_to_count,
    total_fees AS total_fees_native,
    total_fees * p.price AS total_fees_usd,
    COALESCE (
        core_metrics_hourly_id,
        {{ dbt_utils.generate_surrogate_key(
            ['block_timestamp_hour']
        ) }}
    ) AS ez_core_metrics_hourly_id,
    COALESCE(
        s.inserted_timestamp,
        '2000-01-01'
    ) AS inserted_timestamp,
    COALESCE(
        s.modified_timestamp,
        '2000-01-01'
    ) AS modified_timestamp
FROM
    {{ ref('silver_stats__core_metrics_hourly') }}
    s
    LEFT JOIN {{ ref('price__ez_hourly_token_prices') }}
    p
    ON s.block_timestamp_hour = p.hour
    AND p.token_address = '0xe91d153e0b41518a2ce8dd3d7944fa863463a97d' --WXDAI