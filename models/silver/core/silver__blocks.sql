-- depends_on: {{ ref('bronze__blocks') }}
{{ config(
    materialized = 'incremental',
    unique_key = "block_number",
    cluster_by = "block_timestamp::date",
    tags = ['non_realtime'],
    merge_exclude_columns = ["inserted_timestamp"],
    post_hook = "ALTER TABLE {{ this }} ADD SEARCH OPTIMIZATION ON EQUALITY(hash,parent_hash,receipts_root,sha3_uncles,state_root,transactions_root)",
    full_refresh = false
) }}

SELECT
    block_number,
    TRY_TO_NUMBER(
        utils.udf_hex_to_int(
            DATA :baseFeePerGas :: STRING
        )
    ) AS base_fee_per_gas,
    TRY_TO_NUMBER(
        utils.udf_hex_to_int(
            DATA :difficulty :: STRING
        )
    ) AS difficulty,
    DATA :extraData :: STRING AS extra_data,
    TRY_TO_NUMBER(
        utils.udf_hex_to_int(
            DATA :gasLimit :: STRING
        )
    ) AS gas_limit,
    TRY_TO_NUMBER(
        utils.udf_hex_to_int(
            DATA :gasUsed :: STRING
        )
    ) AS gas_used,
    DATA :hash :: STRING AS HASH,
    DATA :logsBloom :: STRING AS logs_bloom,
    DATA :miner :: STRING AS miner,
    TRY_TO_NUMBER(
        utils.udf_hex_to_int(
            DATA :nonce :: STRING
        )
    ) AS nonce,
    TRY_TO_NUMBER(
        utils.udf_hex_to_int(
            DATA :number :: STRING
        )
    ) AS NUMBER,
    DATA :parentHash :: STRING AS parent_hash,
    DATA :receiptsRoot :: STRING AS receipts_root,
    DATA :sha3Uncles :: STRING AS sha3_uncles,
    TRY_TO_NUMBER(
        utils.udf_hex_to_int(
            DATA :size :: STRING
        )
    ) AS SIZE,
    DATA :stateRoot :: STRING AS state_root,
    utils.udf_hex_to_int(
        DATA :timestamp :: STRING
    ) :: TIMESTAMP AS block_timestamp,
    TRY_TO_NUMBER(
        utils.udf_hex_to_int(
            DATA :totalDifficulty :: STRING
        )
    ) AS total_difficulty,
    DATA :transactionsRoot :: STRING AS transactions_root,
    DATA :uncles AS uncles,
    _inserted_timestamp,
    {{ dbt_utils.generate_surrogate_key(
        ['block_number']
    ) }} AS blocks_id,
    SYSDATE() AS inserted_timestamp,
    SYSDATE() AS modified_timestamp,
    '{{ invocation_id }}' AS _invocation_id
FROM

{% if is_incremental() %}
{{ ref('bronze__blocks') }}
WHERE
    _inserted_timestamp >= (
        SELECT
            MAX(_inserted_timestamp) _inserted_timestamp
        FROM
            {{ this }}
    )
{% else %}
    {{ ref('bronze__blocks_fr') }}
{% endif %}

qualify(ROW_NUMBER() over (PARTITION BY block_number
ORDER BY
    _inserted_timestamp DESC)) = 1
