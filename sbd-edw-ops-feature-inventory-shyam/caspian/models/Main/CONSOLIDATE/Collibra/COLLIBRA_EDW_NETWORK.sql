{{
    config(
        enabled=true,
        materialized="view",
        tags=["ops_collibra_prep"],
        schema="CONSOLIDATED",
        alias="VW_EDW_NETWORK"
    )

}}

{{ dbt_utils.union_relations( relations=[ ref('EDW_JDA_NETWORK_VW')]) }}
