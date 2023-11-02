{{
    config(
        enabled=true,
        materialized="view",
        tags=["ops_collibra_prep"],
        schema="CONSOLIDATED",
        alias="VW_EDW_CUSTOMER_ORDER"
    )

}}

{{ dbt_utils.union_relations( relations=[ ref('EDW_JDA_CUSTOMER_ORDER_VW')]) }}
