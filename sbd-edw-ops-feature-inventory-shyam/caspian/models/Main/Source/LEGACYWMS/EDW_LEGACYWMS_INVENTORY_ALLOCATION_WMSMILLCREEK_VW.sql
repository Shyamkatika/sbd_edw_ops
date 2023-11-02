Select
UPPER(CONCAT(coalesce(INVENTORY.LODNUM ::VARCHAR,''),'~',coalesce(INVENTORY.SKU :: VARCHAR,''),'~','WMSMILLCREEK')) AS INVTY_ALLOC_KEY,
'LEGACYWMS' AS {{var('column_srcsyskey')}},
MD5(UPPER(CONCAT(coalesce(INVENTORY.LODNUM::VARCHAR,''),'~',coalesce(INVENTORY.SKU :: VARCHAR,''),'~','WMSMILLCREEK'))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
INVENTORY.LOADDTS AS {{var('column_z3loddtm')}},
INVENTORY.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
'WMSMILLCREEK' as WMS_SCHEMA_NAME,

INVENTORY.CARTON_COUNT AS INVTY_CARTONS_CNT,
INVENTORY.CUBE AS INVTY_STORG_VOL,
INVENTORY.DATE_CODE AS DTE_TRK_NBR,
case when length(trim(UPPER(INVENTORY.INVENTORY_STATUS)))<1 then {{var('default_mapkey')}} when UPPER( INVENTORY.INVENTORY_STATUS)  ='NULL' then  {{var('default_mapkey')}} else UPPER(INVENTORY.INVENTORY_STATUS) end AS INVTY_STAT_LKEY,
INVENTORY.LABELLED AS PROD_IS_LABELLED_FLAG,
INVENTORY.MANUFACTURE_DT AS PROD_MFG_DTE,
INVENTORY.PUTAWAY_DT AS PROD_PUT_AWAY_DTE,
case when length(trim(UPPER(INVENTORY.RECEIPT_TYPE)))<1 then {{var('default_mapkey')}} when UPPER( INVENTORY.RECEIPT_TYPE)  ='NULL' then  {{var('default_mapkey')}} else UPPER(INVENTORY.RECEIPT_TYPE) end AS INVTY_RCPT_TYP_LKEY,
INVENTORY.RECEIVED_DT AS INVTY_RCPT_DTE,
case when length(trim(UPPER(INVENTORY.STORAGE_TYPE)))<1 then {{var('default_mapkey')}} when UPPER( INVENTORY.STORAGE_TYPE)  ='NULL' then  {{var('default_mapkey')}} else UPPER(INVENTORY.STORAGE_TYPE) end AS INVTY_STORG_TYP_LKEY,
INVENTORY.SUB_PALLET_ID AS SUB_PAL_NBR,
INVENTORY.ALLOCATED_QTY AS ALLOC_QTY,
INVENTORY.PALLET_ID AS PAL_ID,
INVENTORY.QTY AS ISS_QTY,
INVENTORY.RECEIPT_NUMBER AS REF_DOC_HDR_NBR,
case when length(trim(UPPER(INVENTORY.SKU)))<1 then {{var('default_mapkey')}} when UPPER (INVENTORY.SKU) ='NULL' then  {{var('default_mapkey')}} else UPPER (INVENTORY.SKU) end AS PROD_KEY,
INVENTORY.WEIGHT AS PROD_WGT,
INVENTORY.SUPPLIER_ID AS SUPLR_NBR,
INVENTORY.VENDOR_NUMBER AS VEND_NBR,
INVENTORY.LODNUM AS PAL_NBR

FROM {{source('WMSMILLCREEK','INVENTORY')}} INVENTORY