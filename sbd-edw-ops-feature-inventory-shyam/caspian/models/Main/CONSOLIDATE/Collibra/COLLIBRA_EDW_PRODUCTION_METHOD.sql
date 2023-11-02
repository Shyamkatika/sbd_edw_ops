{{
    config(
        enabled=true,
        materialized="view",
        tags=["ops_collibra_prep"],
        schema="CONSOLIDATED",
        alias="VW_EDW_PRODUCTION_METHOD"
    )

}}

{{ dbt_utils.union_relations( relations=[ ref('EDW_JDA_PRODUCTION_METHOD_VW')]) }}
