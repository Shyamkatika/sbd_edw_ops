{{
    config(
        enabled=true,
        materialized="view",
        tags=["ops_collibra_prep"],
        schema="CONSOLIDATED",
        alias="VW_EDW_CURRENCY_PROFILE"
    )

}}

{{ dbt_utils.union_relations( relations=[ ref('EDW_JDA_CURRENCY_PROFILE_VW')]) }}
