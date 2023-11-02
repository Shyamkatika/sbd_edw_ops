select
UPPER(CONCAT(coalesce(CYCLE_COUNT.CC_NUMBER::varchar,''),'~',coalesce(CYCLE_COUNT.LOCATION::varchar,''),'~',coalesce(CYCLE_COUNT.SKU::varchar,''),'~','WMSWEB')) AS INVTY_CYCLE_CNT_KEY,
'LEGACYWMS' AS {{var('column_srcsyskey')}},
MD5(UPPER(CONCAT(coalesce(CYCLE_COUNT.CC_NUMBER::varchar,''),'~',coalesce(CYCLE_COUNT.LOCATION::varchar,''),'~',coalesce(CYCLE_COUNT.SKU::varchar,''),'~','WMSWEB'))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
CYCLE_COUNT.LOADDTS AS {{var('column_z3loddtm')}},
CYCLE_COUNT.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
'WMSWEB' as WMS_SCHEMA_NAME,
CYCLE_COUNT.QTY_FOUND	AS 	CYCLE_CNT_START_ON_HAND_QTY,
CYCLE_COUNT.QTY_EXPECTED	AS 	CYCLE_CNT_EXPCT_START_ON_HAND_QTY,
CYCLE_COUNT.ISSUE_DT	AS 	CYCLE_CNT_ACT_DTE,
CYCLE_COUNT.DISCREPANCY_FLAG	AS 	DISCREPANCY_FLAG,
CYCLE_COUNT.DEFAULT_PRIORITY	AS 	DFLT_PRYRT_NBR,
---TRY_TO_DATE(WMTRNXP.TRDTRF,'YYYYMMDD') 
CYCLE_COUNT.COUNT_DT	AS 	CYCLE_CNT_DTE,
CYCLE_COUNT.CC_NUMBER	AS 	CYCLE_CNT_NBR,
case when length(trim(UPPER((CYCLE_COUNT.SKU)))) <1 then {{var('default_mapkey')}} when UPPER(CYCLE_COUNT.SKU)
='NULL' then {{var('default_mapkey')}} else UPPER(CYCLE_COUNT.SKU) end AS PROD_KEY,
CYCLE_COUNT.LOCATION AS SLOC_ID,
CYCLE_COUNT.PUT_ZONE_ID AS PUT_ZONE_ID,
case when length(trim(UPPER((CYCLE_COUNT.COUNT_TYPE)))) <1 then {{var('default_mapkey')}} when UPPER(CYCLE_COUNT.COUNT_TYPE)
IS NULL then {{var('default_mapkey')}} else UPPER(CYCLE_COUNT.COUNT_TYPE) end AS CYCLE_CNT_CLS_LKEY,
CYCLE_COUNT.COUNT_STATUS AS CNT_STAT_TYP,
case when length(trim(UPPER((CYCLE_COUNT.CLOSURE_REASON)))) <1 then {{var('default_mapkey')}} when UPPER(CYCLE_COUNT.CLOSURE_REASON)
='NULL' then {{var('default_mapkey')}} else UPPER(CYCLE_COUNT.CLOSURE_REASON) end AS CLOSURE_RESN_LKEY
FROM {{source('WMSWEB','CYCLE_COUNT')}} CYCLE_COUNT