{{
    config(
        enabled=true,
        materialized="view",
        tags=["ops_collibra_prep"],
        schema="CONSOLIDATED",
        alias="VW_EDW_PRICE_PROFILE"
    )

}}

{{ dbt_utils.union_relations( relations=[ ref('EDW_JDA_PRICE_PROFILE_VW')]) }}