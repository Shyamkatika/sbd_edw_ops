{{
    config(
        enabled=true,
        materialized="view",
        tags=["ops_collibra_prep"],
        schema="CONSOLIDATED",
        alias="VW_EDW_INVENTORY_CYCLE_COUNT_CLASS"
    )

}}

{{ dbt_utils.union_relations( relations=[ ref('EDW_JDAWMS_INVENTORY_CYCLE_COUNT_CLASS_ASIA_VW'),
                                         ref('EDW_JDAWMS_INVENTORY_CYCLE_COUNT_CLASS_EANZ_VW'), 
                                         ref('EDW_JDAWMS_INVENTORY_CYCLE_COUNT_CLASS_LA1_VW'),
                                         ref('EDW_JDAWMS_INVENTORY_CYCLE_COUNT_CLASS_NA1_VW')
                                        ]
                            ) 
}}
