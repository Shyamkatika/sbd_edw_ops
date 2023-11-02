{{
    config(
        enabled=true,
        materialized="view",
        tags=["ops_collibra_prep"],
        schema="CONSOLIDATED",
        alias="VW_EDW_PRODUCT_BOM_COMPONENT"
    )

}}

{{ dbt_utils.union_relations( relations=[ ref('EDW_QADAR_PRODUCT_BOM_COMPONENT_VW'),
                                          ref('EDW_QADBR_PRODUCT_BOM_COMPONENT_VW'),
                                          ref('EDW_QADCH_PRODUCT_BOM_COMPONENT_VW'),
                                          ref('EDW_QADPE_PRODUCT_BOM_COMPONENT_VW'),
                                          ref('EDW_SAPC11_PRODUCT_BOM_COMPONENT_VW'),
                                          ref('EDW_SAPE03_PRODUCT_BOM_COMPONENT_VW'),
                                          ref('EDW_SAPP10_PRODUCT_BOM_COMPONENT_VW'),
                                          ref('EDW_UFIDA_PRODUCT_BOM_COMPONENT_VW')
                                        ]
                            ) 
}}
