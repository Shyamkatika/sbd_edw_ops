{{
    config(
        enabled=true,
        materialized="view",
        tags=["ops_collibra_prep"],
        schema="CONSOLIDATED",
        alias="vw_EDW_DEMAND_SALES_HISTORY"
    )

}}

{{ dbt_utils.union_relations( relations=[ ref('EDW_JDA_DEMAND_SALES_HISTORY_VW')]) }}
