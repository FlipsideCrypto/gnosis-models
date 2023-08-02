{% macro create_aws_gnosis_api() %}
    {{ log(
        "Creating integration for target:" ~ target
    ) }}

    {% if target.name == "prod" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_gnosis_api api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/gnosis-api-prod-rolesnowflakeudfsAF733095-1S5YHE2BPGRAE' api_allowed_prefixes = (
            'https://abjc1ljs1d.execute-api.us-east-1.amazonaws.com/prod/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
        {% elif target.name == "dev" %}
        {% set sql %}
        CREATE api integration IF NOT EXISTS aws_gnosis_api_dev api_provider = aws_api_gateway api_aws_role_arn = 'arn:aws:iam::490041342817:role/gnosis-api-dev-rolesnowflakeudfsAF733095-KZU75JVH7RGG' api_allowed_prefixes = (
            'https://fp4z9mbqa3.execute-api.us-east-1.amazonaws.com/dev/'
        ) enabled = TRUE;
{% endset %}
        {% do run_query(sql) %}
    {% endif %}
{% endmacro %}
