{{
    config(
        enabled=true,
        materialized="view",
        tags=["ops_collibra_prep"],
        schema="CONSOLIDATED",
        alias="VW_EDW_SUPPLY_PLANNING_RESOURCE_PROJECTION"
    )

}}

{{ dbt_utils.union_relations( relations=[ ref('EDW_JDA_SUPPLY_PLANNING_RESOURCE_PROJECTION_VW')]) }}
