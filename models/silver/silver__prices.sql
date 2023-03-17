{{ config(
    materialized = 'view'
) }}

SELECT
    HOUR,
    token_address,
    price
FROM
    {{ source(
        'ethereum',
        'fact_hourly_token_prices'
    ) }}
WHERE
    token_address = LOWER('0x6B175474E89094C44Da98b954EedeAC495271d0F')
