SELECT
UPPER(concat(COALESCE(LADDET.LAD_DATASET,''),'~',COALESCE(LADDET.LAD_NBR,''),'~',COALESCE(LADDET.LAD_LINE,''),'~',COALESCE(LADDET.LAD_SITE,''),'~',COALESCE(LADDET.LAD_LOC,''),'~',COALESCE(LADDET.LAD_REF,''),'~',COALESCE(LADDET.LAD_PART,''),'~',COALESCE(LADDET.LAD_LOT,''))) AS INVTY_ALLOC_KEY,
'QADAR' AS {{var('column_srcsyskey')}},
MD5(UPPER(concat(COALESCE(LADDET.LAD_DATASET,''),'~',COALESCE(LADDET.LAD_NBR,''),'~',COALESCE(LADDET.LAD_LINE,''),'~',COALESCE(LADDET.LAD_SITE,''),'~',COALESCE(LADDET.LAD_LOC,''),'~',COALESCE(LADDET.LAD_REF,''),'~',COALESCE(LADDET.LAD_PART,''),'~',COALESCE(LADDET.LAD_LOT,'')))) AS {{var('column_redhashkey')}},
{{var('default_n')}} AS {{var('column_delfromsrcflag')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
LADDET.LOADDTS AS {{var('column_z3loddtm')}},
LADDET.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
LADDET.LAD_DATASET AS ALLOC_DATA_SET_NAME,
LADDET.LAD_NBR AS REF_DOC_HDR_NBR,
LADDET.LAD_LINE AS REF_DOC_LN_NBR,
case when length(trim(UPPER((LADDET.LAD_SITE)))) <1 then {{var('default_mapkey')}} when UPPER(LADDET.LAD_SITE)
IS NULL then {{var('default_mapkey')}} else UPPER(LADDET.LAD_SITE) end AS LOC_KEY,
--case when length(trim(UPPER((LADDET.LAD_LOC)))) <1 then {{var('default_mapkey')}} when UPPER(LADDET.LAD_LOC)
--IS NULL then {{var('default_mapkey')}} else UPPER(LADDET.LAD_LOC) end AS WHSE_LOC_KEY,
LADDET.LAD_LOC AS WHSE_LOC_NAME,
--case when length(trim(UPPER((LADDET.LAD_REF)))) <1 then {{var('default_mapkey')}} when UPPER(LADDET.LAD_REF)
--IS NULL then {{var('default_mapkey')}} else UPPER(LADDET.LAD_REF) end AS STORG_SUB_LOC_KEY,
LADDET.LAD_REF AS STORG_SUB_LOC_ID,
case when length(trim(UPPER((LADDET.LAD_PART)))) <1 then {{var('default_mapkey')}} when UPPER(LADDET.LAD_PART)
IS NULL then {{var('default_mapkey')}} else UPPER(LADDET.LAD_PART) end AS PROD_KEY,
LADDET.LAD_LOT AS ALLOC_LOT_NBR,
LADDET.LAD_QTY_ALL AS ALLOC_QTY,
LADDET.LAD_QTY_PICK AS PICK_QTY,
LADDET.LAD_QTY_CHG AS ISS_QTY,
case when length(trim(UPPER((LADDET.LAD_ORD_SITE)))) <1 then {{var('default_mapkey')}} when UPPER(LADDET.LAD_ORD_SITE)
IS NULL then {{var('default_mapkey')}} else UPPER(LADDET.LAD_ORD_SITE) end AS DFLT_SITE_LOC_KEY
FROM {{source('QADAR','LADDET')}} LADDET