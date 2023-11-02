{{
    config(
        enabled=true,
        materialized="view",
        tags=["ops_collibra_prep"],
        schema="CONSOLIDATED",
        alias="VW_EDW_DEMAND_FORECASTING_UNIT"
    )

}}

{{ dbt_utils.union_relations( relations=[ ref('EDW_JDA_DEMAND_FORECASTING_UNIT_VW')]) }}
