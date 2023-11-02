{{
    config(
        enabled=true,
        materialized="view",
        tags=["ops_collibra_prep"],
        schema="CONSOLIDATED",
        alias="VW_EDW_RESOURCE_LOAD_DETAILS"
    )

}}

{{ dbt_utils.union_relations( relations=[ ref('EDW_JDA_RESOURCE_LOAD_DETAILS_VW')]) }}
