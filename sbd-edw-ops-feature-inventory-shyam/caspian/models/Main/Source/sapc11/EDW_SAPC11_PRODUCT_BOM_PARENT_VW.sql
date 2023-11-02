Select
UPPER(CONCAT(coalesce(STKO.STKOZ::varchar,''),'~',coalesce(STKO.STLAL::varchar,''),'~' ,coalesce(STKO.STLTY::varchar,''),'~' ,coalesce(STKO.STLNR::varchar,''),'~' ,coalesce(MAST.MATNR::varchar,''),'~' ,coalesce(MAST.WERKS::varchar,''),'~' ,coalesce(MAST.STLAN::varchar,''),'~',coalesce(MAST.STLNR::varchar,''),'~',coalesce(MAST.STLAL::varchar,''))) AS PROD_BOM_PRNT_KEY,
'SAPC11' AS {{var('column_srcsyskey')}},
MD5(UPPER(CONCAT(coalesce(STKO.STKOZ::varchar,''),'~',coalesce(STKO.STLAL::varchar,''),'~' ,coalesce(STKO.STLTY::varchar,''),'~' ,coalesce(STKO.STLNR::varchar,''),'~' ,coalesce(MAST.MATNR::varchar,''),'~' ,coalesce(MAST.WERKS::varchar,''),'~' ,coalesce(MAST.STLAN::varchar,''),'~',coalesce(MAST.STLNR::varchar,''),'~',coalesce(MAST.STLAL::varchar,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
MAST.LOADDTS AS {{var('column_z3loddtm')}},
MAST.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
STKO.AEHLP as DTE_SHFT_HIER_INDCTR,
CASE WHEN STKO.ALEKZ = 'X' THEN 'Y' ELSE 'N' END AS ALE_DATA_DIST_FLAG,
STKO.GUIDX::VARCHAR as GLB_ID_FOR_BOM_HDR_CHG,
CASE WHEN STKO.LKENZ = 'X' THEN 'Y' ELSE 'N' END AS BOM_DEL_FLAG,
CASE WHEN STKO.LOEKZ = 'X' THEN 'Y' ELSE 'N' END AS ARCHIVING_SPECIFIC_BOM_DEL_FLAG,
STKO.DVNAM as LAST_SHFT_USER_NAME,
STKO.AENNR as BOM_UPD_CHG_NBR,
STKO.BMENG as BASE_QTY,
TRY_TO_DATE(STKO.DATUV,'YYYYMMDD') as PROD_BOM_EFF_DTE,
TRY_TO_DATE(STKO.DVDAT,'YYYYMMDD') as LAST_SHFT_DTE,
STKO.LABOR as LABORATORY_OFFICE_NAME,
STKO.STKOZ as BOM_ITRL_CNTR_NBR,
STKO.STKTX as ALT_BOM_TEXT,
STKO.VGKZL as PREV_BOM_HDR_CNTR,
STKO.TECHV as PRODTN_STAT,
CASE WHEN STKO.CADKZ = 'X' THEN 'Y' ELSE 'N' END AS CAD_FLAG,
--STKO.VALID_TO as VALID_END_DTE,
--STKO.ECN_TO as CHG_NBR,
--STKO.BOM_VERSN as BOM_VER_NBR,
--STKO.VERSNST as BOM_VER_STAT,
--STKO.VERSNLASTIND as LATEST_BOM_VER_FLAG,
--STKO.LASTCHANGEDATETIME as UTC_TM_STAMP,
--STKO.BOM_AIN_IND as BOM_TO_AIN_HNDVR,
--STKO.BOM_PREV_VERSN as BOM_PRED_VER_NBR,
--STKO.DUMMY_STKO_INCL_EEW_PS as EXTENSIBILITY_ELMNT_DTE,
MAST.LOSVN as MIN_LOT_RANGE_NBR,
MAST.LOSBS as MAX_LOT_RANGE_NBR,
CASE WHEN MAST.CSLTY = 'X' THEN 'Y' ELSE 'N' END AS IS_PROD_CONFIGURABLE_FLAG,
case when length(trim(UPPER((STKO.BMEIN)))) <1 then {{var('default_mapkey')}} when UPPER(STKO.BMEIN)
IS NULL then {{var('default_mapkey')}} else UPPER(STKO.BMEIN) end AS BASE_UOM_KEY,

case when length(trim(UPPER((STKO.WRKAN)))) <1 then {{var('default_mapkey')}} when UPPER(STKO.WRKAN)
IS NULL then {{var('default_mapkey')}} else UPPER(STKO.WRKAN) end AS ALT_BOM_PLANT_LOC_KEY,


case when length(trim(UPPER((STKO.STLST)))) <1 then {{var('default_mapkey')}} when UPPER(STKO.STLST)
IS NULL then {{var('default_mapkey')}} else UPPER(STKO.STLST) end AS BOM_STAT_LKEY,

case when length(trim(UPPER((STKO.STLTY)))) <1 then {{var('default_mapkey')}} when UPPER(STKO.STLTY)
IS NULL then {{var('default_mapkey')}} else UPPER(STKO.STLTY) end AS BOM_CTGY_LKEY,


case when length(trim(UPPER((STKO.LTXSP)))) <1 then {{var('default_mapkey')}} when UPPER(STKO.LTXSP)
IS NULL then {{var('default_mapkey')}} else UPPER(STKO.LTXSP) end AS LANG_LKEY,

case when length(trim(UPPER((MAST.WERKS)))) <1 then {{var('default_mapkey')}} when UPPER(MAST.WERKS)
IS NULL then {{var('default_mapkey')}} else UPPER(MAST.WERKS) end AS LOC_KEY,

case when length(trim(UPPER((MAST.STLAN)))) <1 then {{var('default_mapkey')}} when UPPER(MAST.STLAN)
IS NULL then {{var('default_mapkey')}} else UPPER(MAST.STLAN) end AS BOM_USAGE_LKEY,

MAST.STLNR AS BOM_VAL,

MAST.STLAL AS ALT_BOM_VAL,

case when length(trim(UPPER((MAST.MATNR)))) <1 then {{var('default_mapkey')}} when UPPER(MAST.MATNR)
IS NULL then {{var('default_mapkey')}} else UPPER(MAST.MATNR) end AS PROD_KEY


FROM {{source('SAPC11','MAST')}} MAST
left join {{source('SAPC11','STKO')}} STKO on MAST.STLNR = STKO.STLNR AND MAST.STLAL = STKO.STLAL AND STKO.STLTY = 'M'
WHERE MAST.MANDT={{var('sapc11mandtftr')}}