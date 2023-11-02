{{
    config(
        enabled=true,
        materialized="view",
        tags=["ops_collibra_prep"],
        schema="CONSOLIDATED",
        alias="VW_EDW_DEMAND_FORECAST_HISTORY"
    )

}}

{{ dbt_utils.union_relations( relations=[ ref('EDW_JDA_DEMAND_FORECAST_HISTORY_VW')]) }}
