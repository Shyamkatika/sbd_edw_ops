{{
    config(
        enabled=true,
        materialized="view",
        tags=["ops_collibra_prep"],
        schema="CONSOLIDATED",
        alias="VW_EDW_SUPPLY_PLANNING_PRODUCT_VIEW"
    )

}}

{{ dbt_utils.union_relations( relations=[ ref('EDW_JDA_SUPPLY_PLANNING_PRODUCT_VIEW_VW'), ref('EDW_JDA_SUPPLY_PLANNING_PRODUCT_VIEW_SKU_VW') ]) }}
