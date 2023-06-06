{{ config(
    materialized = 'view'
) }}

SELECT
    system_created_at,
    insert_date,
    blockchain,
    address,
    creator,
    label_type,
    label_subtype,
    address_name,
    project_name
FROM
    {{ source(
        'crosschain',
        'address_labels'
    ) }}
WHERE
    blockchain = 'gnosis'
    AND address LIKE '0x%'