{{
    config(
        enabled=true,
        materialized="view",
        tags=["ops_collibra_prep"],
        schema="CONSOLIDATED",
        alias="VW_EDW_SUPPLY_PLANNED_RECOMMENDED_SHIPMENT"
    )

}}

{{ dbt_utils.union_relations( relations=[ ref('EDW_JDA_SUPPLY_PLANNED_RECOMMENDED_SHIPMENT_VW')]) }}
