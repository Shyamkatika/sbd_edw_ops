{{
    config(
        enabled=true,
        materialized="view",
        tags=["ops_collibra_prep"],
        schema="CONSOLIDATED",
        alias="VW_EDW_INVENTORY_STORAGE_LOCATION"
    )

}}

{{ dbt_utils.union_relations( relations=[ ref('EDW_LAWSONMAC_INVENTORY_STORAGE_LOCATION_VW'),
                                          ref('EDW_LEGACYWMS_INVENTORY_STORAGE_LOCATION_WMSBREWSTER_VW'),
                                          ref('EDW_LEGACYWMS_INVENTORY_STORAGE_LOCATION_WMSEASTLONGMEADOW_VW'),
                                          ref('EDW_LEGACYWMS_INVENTORY_STORAGE_LOCATION_WMSFONTANA_VW'),
                                          ref('EDW_LEGACYWMS_INVENTORY_STORAGE_LOCATION_WMSFORTMILL_VW'),
                                          ref('EDW_LEGACYWMS_INVENTORY_STORAGE_LOCATION_WMSFONTANA_VW'),
                                          ref('EDW_LEGACYWMS_INVENTORY_STORAGE_LOCATION_WMSJACKSON_VW'),
                                          ref('EDW_LEGACYWMS_INVENTORY_STORAGE_LOCATION_WMSKANNAPOLIS_VW'),
                                          ref('EDW_LEGACYWMS_INVENTORY_STORAGE_LOCATION_WMSMILLCREEK_VW'),                                                                                    
                                          ref('EDW_LEGACYWMS_INVENTORY_STORAGE_LOCATION_WMSNORTHLAKE_VW'),
                                          ref('EDW_LEGACYWMS_INVENTORY_STORAGE_LOCATION_WMSWEB_VW'),
                                          ref('EDW_SAPC11_INVENTORY_STORAGE_LOCATION_VW'),
                                          ref('EDW_SAPE03_INVENTORY_STORAGE_LOCATION_VW'),
                                          ref('EDW_SAPP10_INVENTORY_STORAGE_LOCATION_VW')
                                        ]
                            ) 
}}
