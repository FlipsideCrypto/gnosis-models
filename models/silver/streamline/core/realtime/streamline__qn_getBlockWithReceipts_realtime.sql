{{ config (
    materialized = "view",
    post_hook = if_data_call_function(
        func = "{{this.schema}}.udf_bulk_json_rpc(object_construct('sql_source', '{{this.identifier}}', 'external_table', 'qn_getBlockWithReceipts', 'sql_limit', {{var('sql_limit','150000')}}, 'producer_batch_size', {{var('producer_batch_size','75000')}}, 'worker_batch_size', {{var('worker_batch_size','15000')}}, 'batch_call_limit', {{var('batch_call_limit','10')}}, 'call_type', 'batch'))",
        target = "{{this.schema}}.{{this.identifier}}"
    )
) }}

WITH blocks AS (

    SELECT
        block_number :: STRING AS block_number
    FROM
        {{ ref("streamline__blocks") }}
    EXCEPT
    SELECT
        block_number :: STRING
    FROM
        {{ ref("streamline__complete_qn_getBlockWithReceipts") }}
)
SELECT
    PARSE_JSON(
        CONCAT(
            '{"jsonrpc": "2.0",',
            '"method": "qn_getBlockWithReceipts", "params":["',
            REPLACE(
                concat_ws(
                    '',
                    '0x',
                    to_char(
                        block_number :: INTEGER,
                        'XXXXXXXX'
                    )
                ),
                ' ',
                ''
            ),
            '"],"id":"',
            block_number :: INTEGER,
            '"}'
        )
    ) AS request
FROM
    blocks
ORDER BY
    block_number ASC
