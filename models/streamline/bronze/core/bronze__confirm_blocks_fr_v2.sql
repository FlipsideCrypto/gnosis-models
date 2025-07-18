{# Log configuration details #}
{{ fsc_evm.log_model_details() }}

{# Set up dbt configuration #}
{{ config (
    materialized = 'view',
    tags = ['bronze','core','confirm_blocks','phase_1']
) }}

{# Main query starts here #}
{{ fsc_evm.streamline_external_table_query_fr(
    source_name = 'confirm_blocks',
    source_version = 'v2',
    error_code = true
) }}