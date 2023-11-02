{{
    config(
        enabled=true,
        materialized="view",
        tags=["ops_collibra_prep"],
        schema="CONSOLIDATED",
        alias="VW_EDW_INVENTORY_PHYSICAL_COUNT"
    )

}}

{{ dbt_utils.union_relations( relations=[ ref('EDW_QADBR_INVENTORY_PHYSICAL_COUNT_VW'),
                                          ref('EDW_SAPC11_INVENTORY_PHYSICAL_COUNT_VW'),
                                          ref('EDW_SAPE03_INVENTORY_PHYSICAL_COUNT_VW'),
                                          ref('EDW_SAPP10_INVENTORY_PHYSICAL_COUNT_VW'),
                                          ref('EDW_UFIDA_INVENTORY_PHYSICAL_COUNT_VW')
                                        ]
                            ) 
}}
