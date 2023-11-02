{{
    config(
        enabled=true,
        materialized="view",
        tags=["ops_collibra_prep"],
        schema="CONSOLIDATED",
        alias="VW_EDW_INVENTORY_RESERVATION_HEADER"
    )

}}

{{ dbt_utils.union_relations( relations=[ ref('EDW_QADBR_INVENTORY_RESERVATION_HEADER_VW'),
                                         ref('EDW_SAPC11_INVENTORY_RESERVATION_HEADER_VW'),
                                         ref('EDW_SAPE03_INVENTORY_RESERVATION_HEADER_VW'),
                                         ref('EDW_SAPP10_INVENTORY_RESERVATION_HEADER_VW')
                                        ]
                            ) 
}}
