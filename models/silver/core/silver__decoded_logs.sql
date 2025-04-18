-- depends_on: {{ ref('bronze__decoded_logs') }}
{{ config (
    materialized = "incremental",
    unique_key = ['block_number', 'event_index'],
    cluster_by = "block_timestamp::date",
    incremental_predicates = ["dynamic_range", "block_number"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION",
    merge_exclude_columns = ["inserted_timestamp"],
    full_refresh = false,
    tags = ['decoded_logs','reorg']
) }}

WITH base_data AS (

    SELECT
        block_number :: INTEGER AS block_number,
        SPLIT(
            id,
            '-'
        ) [0] :: STRING AS tx_hash,
        SPLIT(
            id,
            '-'
        ) [1] :: INTEGER AS event_index,
        DATA :name :: STRING AS event_name,
        LOWER(
            DATA :address :: STRING
        ) :: STRING AS contract_address,
        DATA AS decoded_data,
        id :: STRING AS _log_id,
        TO_TIMESTAMP_NTZ(_inserted_timestamp) AS _inserted_timestamp
    FROM

{% if is_incremental() %}
{{ ref('bronze__decoded_logs') }}
WHERE
    TO_TIMESTAMP_NTZ(_inserted_timestamp) >= (
        SELECT
            MAX(_inserted_timestamp) - INTERVAL '2 hours'
        FROM
            {{ this }}
    )
    AND DATA NOT ILIKE '%Event topic is not present in given ABI%'
{% else %}
    {{ ref('bronze__decoded_logs_fr') }}
WHERE
    DATA NOT ILIKE '%Event topic is not present in given ABI%'
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number, event_index
ORDER BY
    _inserted_timestamp DESC, _partition_by_created_date DESC)) = 1
),
transformed_logs AS (
    SELECT
        block_number,
        tx_hash,
        event_index,
        contract_address,
        event_name,
        decoded_data,
        _inserted_timestamp,
        _log_id,
        utils.udf_transform_logs(decoded_data) AS transformed
    FROM
        base_data
),
FINAL AS (
    SELECT
        b.tx_hash,
        b.block_number,
        b.event_index,
        b.event_name,
        b.contract_address,
        b.decoded_data,
        transformed,
        b._log_id,
        b._inserted_timestamp,
        OBJECT_AGG(
            DISTINCT CASE
                WHEN v.value :name = '' THEN CONCAT(
                    'anonymous_',
                    v.index
                )
                ELSE v.value :name
            END,
            v.value :value
        ) AS decoded_flat
    FROM
        transformed_logs b,
        LATERAL FLATTEN(
            input => transformed :data
        ) v
    GROUP BY
        b.tx_hash,
        b.block_number,
        b.event_index,
        b.event_name,
        b.contract_address,
        b.decoded_data,
        transformed,
        b._log_id,
        b._inserted_timestamp
),
new_records AS (
    SELECT
        b.tx_hash,
        b.block_number,
        b.event_index,
        b.event_name,
        b.contract_address,
        b.decoded_data,
        b.transformed,
        b._log_id,
        b._inserted_timestamp,
        b.decoded_flat,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        topics,
        DATA,
        event_removed :: STRING AS event_removed,
        tx_status,
        CASE
            WHEN block_timestamp IS NULL THEN TRUE
            ELSE FALSE
        END AS is_pending
    FROM
        FINAL b
        LEFT JOIN {{ ref('silver__logs') }} USING (
            block_number,
            _log_id
        )
)

{% if is_incremental() %},
missing_data AS (
    SELECT
        t.tx_hash,
        t.block_number,
        t.event_index,
        t.event_name,
        t.contract_address,
        t.decoded_data,
        t.transformed,
        t._log_id,
        GREATEST(
            TO_TIMESTAMP_NTZ(
                t._inserted_timestamp
            ),
            TO_TIMESTAMP_NTZ(
                l._inserted_timestamp
            )
        ) AS _inserted_timestamp,
        t.decoded_flat,
        l.block_timestamp,
        l.origin_function_signature,
        l.origin_from_address,
        l.origin_to_address,
        l.topics,
        l.data,
        l.event_removed :: STRING AS event_removed,
        l.tx_status,
        FALSE AS is_pending
    FROM
        {{ this }}
        t
        INNER JOIN {{ ref('silver__logs') }}
        l USING (
            block_number,
            _log_id
        )
    WHERE
        t.is_pending
        AND l.block_timestamp IS NOT NULL
)
{% endif %},
complete_data AS (
    SELECT
        tx_hash,
        block_number,
        event_index,
        event_name,
        contract_address,
        decoded_data,
        transformed,
        _log_id,
        _inserted_timestamp,
        decoded_flat,
        block_timestamp,
        origin_function_signature,
        origin_from_address,
        origin_to_address,
        topics,
        DATA,
        event_removed,
        tx_status,
        is_pending,
        {{ dbt_utils.generate_surrogate_key(
            ['tx_hash', 'event_index']
        ) }} AS decoded_logs_id,
        SYSDATE() AS inserted_timestamp,
        SYSDATE() AS modified_timestamp,
        '{{ invocation_id }}' AS _invocation_id
    FROM
        new_records

{% if is_incremental() %}
UNION
SELECT
    tx_hash,
    block_number,
    event_index,
    event_name,
    contract_address,
    decoded_data,
    transformed,
    _log_id,
    _inserted_timestamp,
    decoded_flat,
    block_timestamp,
    origin_function_signature,
    origin_from_address,
    origin_to_address,
    topics,
    DATA,
    event_removed,
    tx_status,
    is_pending,
    {{ dbt_utils.generate_surrogate_key(
        ['tx_hash', 'event_index']
    ) }} AS decoded_logs_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    missing_data
{% endif %}
)
SELECT
    *
FROM
    complete_data qualify(ROW_NUMBER() over (PARTITION BY block_number, event_index
ORDER BY
    _inserted_timestamp DESC, is_pending ASC)) = 1