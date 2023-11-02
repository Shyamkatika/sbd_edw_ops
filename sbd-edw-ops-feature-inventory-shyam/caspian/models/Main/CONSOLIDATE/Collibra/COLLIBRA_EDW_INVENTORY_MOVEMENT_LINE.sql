{{
    config(
        enabled=true,
        materialized="view",
        tags=["ops_collibra_prep"],
        schema="CONSOLIDATED",
        alias="VW_EDW_INVENTORY_MOVEMENT_LINE"
    )

}}

{{ dbt_utils.union_relations( relations=[ ref('EDW_SAPC11_INVENTORY_MOVEMENT_LINE_VW'),
                                          ref('EDW_SAPE03_INVENTORY_MOVEMENT_LINE_VW'),
                                          ref('EDW_SAPP10_INVENTORY_MOVEMENT_LINE_VW')
                                        ]
                            ) 
}}
