{{
    config(
        enabled=true,
        materialized="view",
        tags=["ops_collibra_prep"],
        schema="CONSOLIDATED",
        alias="VW_EDW_PLANNING_CALENDAR"
    )

}}

{{ dbt_utils.union_relations( relations=[ ref('EDW_JDA_PLANNING_CALENDAR_VW'),ref('EDW_JDA_PLANNING_CALENDAR_BACK_VW')]) }}
