{{
    config(
        enabled=true,
        materialized="view",
        tags=["ops_collibra_prep"],
        schema="CONSOLIDATED",
        alias="VW_EDW_DFU_SKU_FORECAST"
    )

}}

{{ dbt_utils.union_relations( relations=[ ref('EDW_JDA_DFU_SKU_FORECAST_VW')]) }}
