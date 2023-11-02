{{
    config(
        enabled=true,
        materialized="view",
        tags=["ops_collibra_prep"],
        schema="CONSOLIDATED",
        alias="VW_EDW_INVENTORY_ON_HAND_SNAPSHOT"
    )

}}

{{ dbt_utils.union_relations( relations=[ ref('EDW_LAWSONMAC_INVENTORY_ON_HAND_SNAPSHOT_VW'),
                                          ref('EDW_QADAR_INVENTORY_ON_HAND_SNAPSHOT_VW'),
                                          ref('EDW_QADCH_INVENTORY_ON_HAND_SNAPSHOT_VW'),
                                          ref('EDW_QADPE_INVENTORY_ON_HAND_SNAPSHOT_VW'),
                                          ref('EDW_SAPC11_INVENTORY_ON_HAND_SNAPSHOT_VW'),
                                          ref('EDW_SAPC11_INVENTORY_ON_HAND_SNAPSHOT_HIST_VW'),
                                          ref('EDW_SAPE03_INVENTORY_ON_HAND_SNAPSHOT_VW'),
                                          ref('EDW_SAPE03_INVENTORY_ON_HAND_SNAPSHOT_HIST_VW'),
                                          ref('EDW_SAPP10_INVENTORY_ON_HAND_SNAPSHOT_VW'),
                                          ref('EDW_SAPP10_INVENTORY_ON_HAND_SNAPSHOT_HIST_VW')
                                        ]
                            ) 
}}
