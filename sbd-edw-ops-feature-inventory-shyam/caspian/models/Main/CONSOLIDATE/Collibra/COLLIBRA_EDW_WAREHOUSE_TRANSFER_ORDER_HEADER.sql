{{
    config(
        enabled=true,
        materialized="view",
        tags=["ops_collibra_prep"],
        schema="CONSOLIDATED",
        alias="VW_EDW_WAREHOUSE_TRANSFER_ORDER_HEADER"
    )

}}

{{ dbt_utils.union_relations( relations=[ ref('EDW_JDAWMS_WAREHOUSE_TRANSFER_ORDER_HEADER_ASIA_VW'),
                                         ref('EDW_JDAWMS_WAREHOUSE_TRANSFER_ORDER_HEADER_EANZ_VW'), 
                                         ref('EDW_JDAWMS_WAREHOUSE_TRANSFER_ORDER_HEADER_LA1_VW'),
                                         ref('EDW_JDAWMS_WAREHOUSE_TRANSFER_ORDER_HEADER_NA1_VW'),
                                         ref('EDW_SAPC11_WAREHOUSE_TRANSFER_ORDER_HEADER_VW'),
                                         ref('EDW_SAPE03_WAREHOUSE_TRANSFER_ORDER_HEADER_VW'),
                                         ref('EDW_SAPP10_WAREHOUSE_TRANSFER_ORDER_HEADER_VW')
                                         ]
                            ) 
}}
