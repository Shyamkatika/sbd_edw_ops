{{
    config(
        enabled=true,
        materialized="view",
        tags=["ops_collibra_prep"],
        schema="CONSOLIDATED",
        alias="VW_EDW_SUPPLY_PLANNING_DYNAMIC_DEPLOYMENT_SOURCE"
    )

}}

{{ dbt_utils.union_relations( relations=[ ref('EDW_JDA_SUPPLY_PLANNING_DYNAMIC_DEPLOYMENT_SOURCE_VW')]) }}
