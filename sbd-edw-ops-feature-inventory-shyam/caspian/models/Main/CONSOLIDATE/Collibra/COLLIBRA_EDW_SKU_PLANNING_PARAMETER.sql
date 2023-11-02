{{
    config(
        enabled=true,
        materialized="view",
        tags=["ops_collibra_prep"],
        schema="CONSOLIDATED",
        alias="VW_EDW_SKU_PLANNING_PARAMETER"
    )

}}

{{ dbt_utils.union_relations( relations=[ ref('EDW_JDA_SKU_PLANNING_PARAMETER_VW')]) }}
