{% macro decoded_logs_cleanup() %}
    {% set sql %}
DELETE FROM
    {{ ref('silver__decoded_logs') }}
    d
WHERE
    _inserted_timestamp > DATEADD('hour', -48, SYSDATE())
    AND NOT EXISTS (
        SELECT
            1
        FROM
            {{ ref('silver__logs') }}
            l
        WHERE
            d.block_number = l.block_number
            AND d.tx_hash = l.tx_hash
            AND d.event_index = l.event_index
            AND d.contract_address = l.contract_address
            AND d.topics [0] :: STRING = l.topics [0] :: STRING
    );
{% endset %}
    {% do run_query(sql) %}
{% endmacro %}
