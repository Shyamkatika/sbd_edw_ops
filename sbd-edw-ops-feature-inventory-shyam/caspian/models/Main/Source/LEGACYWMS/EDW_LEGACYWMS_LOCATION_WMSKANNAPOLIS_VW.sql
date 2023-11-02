select
CONCAT(coalesce(LOCATION.LOCATION ::VARCHAR,''),'~','WMSKANNAPOLIS') AS LOC_KEY,
'LEGACYWMS' AS {{var('column_srcsyskey')}},
MD5(CONCAT(coalesce(LOCATION.LOCATION ::VARCHAR,''),'~','WMSKANNAPOLIS')) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
LOCATION.LOADDTS AS {{var('column_z3loddtm')}},
LOCATION.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
'WMSKANNAPOLIS' as WMS_SCHEMA_NAME,
LOCATION.AISLE AS AISLE_ID,
LOCATION.BUILDING_NUMBER as SLOC_BUILDING_NBR,
LOCATION.CALC_SKIDS as SKID_NBR,
LOCATION.CAROUSEL_FACE AS CAROUSEL_FACE_ID,
LOCATION.CAROUSEL_LEVEL AS CAROUSEL_HGT,
LOCATION.CAROUSEL_NUMBER as CAROUSEL_QTY, 
case when length(trim(UPPER((LOCATION.CAROUSEL_POD)))) <1 then {{var('default_mapkey')}} when UPPER(LOCATION.CAROUSEL_POD)
IS NULL then {{var('default_mapkey')}} else UPPER(LOCATION.CAROUSEL_POD) end AS CAROUSEL_GRP_LKEY,
LOCATION.CAROUSEL_SLOT AS CAROUSEL_SLOT_ID,
LOCATION.CART_NAME as CART_NBR,
LOCATION.CART_SLOT as CART_SLOT_NBR,
LOCATION.COUNT_ATTEMPTS as ALLOWABLE_CNT_NBR,
LOCATION.CUBE as STORG_VOL,
LOCATION.DISABLED as DSBL_LOC_FLAG,
--LOCATION.EST_AUTOSTORE_BINS as EST_AUTOSTORE_BIN_NBR,
LOCATION.HEIGHT AS SLOC_HGT,
Location.INVENTORY_STATUS as INVTY_STAT_TYP,
LOCATION.LANE_NUMBER as LANE_NBR,
LOCATION.LAST_CC_DT as LAST_CYCLE_CNT_DTE,
LOCATION.LAST_PICK_DT as LAST_PICK_DTE,
LOCATION.LAST_PUTAWAY_DT as LAST_PUTAWAY_DTE,
LOCATION.LENGTH as SLOC_LEN,
LOCATION.LOCATION AS LOC_ID,
LOCATION.LOCATION_CONFIRMATION AS CONFIRM_DIST_LOC_CD,
LOCATION.LOCATION_MAP_CODE AS LOC_CD,
case when length(trim(UPPER((LOCATION.LOCATION_RESERVE_TYPE)))) <1 then {{var('default_mapkey')}} when UPPER(LOCATION.LOCATION_RESERVE_TYPE)
IS NULL  then {{var('default_mapkey')}} when UPPER(LOCATION.LOCATION_RESERVE_TYPE) = 'NULL'  then {{var('default_mapkey')}} else UPPER(LOCATION.LOCATION_RESERVE_TYPE) end AS RSRV_TYP_LKEY,
LOCATION.LOCATION_SKIDS as LOC_SKID_NBR,
case when length(trim(UPPER((LOCATION.LOCATION_SUB_TYPE)))) <1 then {{var('default_mapkey')}} when UPPER(LOCATION.LOCATION_SUB_TYPE)
IS NULL then {{var('default_mapkey')}} else UPPER(LOCATION.LOCATION_SUB_TYPE) end AS SUB_LOC_TYP_LKEY,
LOCATION.LOCATION_TYPE AS PROD_LOC_TYP,
LOCATION.MAX_QTY AS MAX_PROD_QTY,
LOCATION.MAX_SKIDS AS MAX_SKID_NBR,
LOCATION.MAX_WEIGHT as MAX_SFTY_ALLOWABLE_WGT,
LOCATION.OLD_LOCATION as OLD_SKU_LOC_NAME,
LOCATION.OLD_ROW as OLD_LOC_NBR,
LOCATION.PALLET_ID as PAL_ID,
LOCATION.PENDING_CUBE as REMAIN_VOL_QTY,
LOCATION.PENDING_QTY as PENDING_QTY,
LOCATION.PENDING_SKIDS as PENDING_SKID_NBR,
LOCATION.PICK_FACE_FLAG as PICK_FACE_FLAG,
LOCATION.PICK_LOCKED as LOC_PICK_LOCK_FLAG,
case when length(trim(UPPER((LOCATION.PICK_MODULE)))) <1 then {{var('default_mapkey')}} when UPPER(LOCATION.PICK_MODULE)
IS NULL then {{var('default_mapkey')}} else UPPER(LOCATION.PICK_MODULE) end AS PICK_MODULE_TYP_LKEY,
LOCATION.PICK_TRAVEL_SEQUENCE as PICK_ITEM_TRAVEL_SEQ_NBR,
-- case when length(trim(UPPER((LOCATION.PICK_UOM)))) <1 then {{var('default_mapkey')}} when UPPER(LOCATION.PICK_UOM)
-- IS NULL then {{var('default_mapkey')}} else UPPER(LOCATION.PICK_UOM) end AS DFLT_UOM_KEY,
LOCATION.PTP_BY_PALLET as PROCURE_TO_PAY_PAL_NBR,
LOCATION.PUT_LIST_ID AS PUTAWAY_ITEM_LOC_ID,
LOCATION.PUT_LOCKED as LOC_PUT_LOCK_FLAG,
LOCATION.PUT_TRAVEL_SEQUENCE as PUTAWAY_PROD_TRAVEL_SEQ_NBR,
LOCATION.PUT_ZONE_ID AS SKU_PUTAWAY_ZONE_ID,
LOCATION.REPLENISH_LOCATION AS PROD_REPLENISH_LOC_ID,
LOCATION.REPLENISH_QTY as PROD_REPLENISH_QTY,
LOCATION.REPLENISH_UOM AS REPLENISH_UOM_CD,
LOCATION.SCATTER_BOX as SCATTER_BOX_NBR,
LOCATION.SKU AS SKU_ID,
LOCATION.VELOCITY_CODE as VELOCITY_CD_NBR,
LOCATION.WA_ID as WHSE_NBR,
LOCATION.WIDTH as SLOC_WDTH
FROM {{source('WMSKANNAPOLIS','LOCATION')}} LOCATION