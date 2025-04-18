WITH base AS (
    SELECT
        block_number,
        MIN(modified_timestamp) AS _inserted_timestamp,
        COUNT(*) AS tx_count
    FROM
        {{ ref('core__fact_transactions') }}

{% if is_incremental() %}
WHERE
    modified_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% endif %}
GROUP BY
    block_number
)
SELECT
    *,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number']
    ) }} AS tx_count_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM
    base
