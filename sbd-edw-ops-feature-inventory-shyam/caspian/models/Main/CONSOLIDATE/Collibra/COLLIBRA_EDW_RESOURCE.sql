{{
    config(
        enabled=true,
        materialized="view",
        tags=["ops_collibra_prep"],
        schema="CONSOLIDATED",
        alias="VW_EDW_RESOURCE"
    )

}}

{{ dbt_utils.union_relations( relations=[ ref('EDW_JDA_RESOURCE_VW')]) }}
