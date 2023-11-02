{{
    config(
        enabled=true,
        materialized="view",
        tags=["ops_collibra_prep"],
        schema="CONSOLIDATED",
        alias="VW_EDW_VEHICLE_LOAD_HEADER"
    )

}}

{{ dbt_utils.union_relations( relations=[ ref('EDW_JDA_VEHICLE_LOAD_HEADER_VW')]) }}
