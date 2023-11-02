{{
    config(
        enabled=true,
        materialized="view",
        tags=["ops_collibra_prep"],
        schema="CONSOLIDATED",
        alias="VW_EDW_INVENTORY_ALLOCATION"
    )

}}

{{ dbt_utils.union_relations( relations=[ ref('EDW_LEGACYWMS_INVENTORY_ALLOCATION_WMSBREWSTER_VW'),
                                         ref('EDW_LEGACYWMS_INVENTORY_ALLOCATION_WMSEASTLONGMEADOW_VW'), 
                                         ref('EDW_LEGACYWMS_INVENTORY_ALLOCATION_WMSFONTANA_VW'),
                                         ref('EDW_LEGACYWMS_INVENTORY_ALLOCATION_WMSFORTMILL_VW'),
                                         ref('EDW_LEGACYWMS_INVENTORY_ALLOCATION_WMSJACKSON_VW'),
                                         ref('EDW_LEGACYWMS_INVENTORY_ALLOCATION_WMSKANNAPOLIS_VW'),
                                         ref('EDW_LEGACYWMS_INVENTORY_ALLOCATION_WMSMILLCREEK_VW'),
                                         ref('EDW_LEGACYWMS_INVENTORY_ALLOCATION_WMSNORTHLAKE_VW'),
                                         ref('EDW_LEGACYWMS_INVENTORY_ALLOCATION_WMSWEB_VW'),
                                         ref('EDW_QADAR_INVENTORY_ALLOCATION_VW'),
                                         ref('EDW_QADCH_INVENTORY_ALLOCATION_VW'),
                                         ref('EDW_QADPE_INVENTORY_ALLOCATION_VW')

                                        ]
                            ) 
}}
