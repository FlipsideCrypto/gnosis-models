{% macro run_sp_create_prod_clone() %}
    {% set clone_query %}
    call gnosis._internal.create_prod_clone(
        'gnosis',
        'gnosis_dev',
        'gnosis_dev_owner'
    );
{% endset %}
    {% do run_query(clone_query) %}
{% endmacro %}
