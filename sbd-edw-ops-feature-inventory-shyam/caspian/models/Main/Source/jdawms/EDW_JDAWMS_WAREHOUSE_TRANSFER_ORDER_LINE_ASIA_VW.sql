SELECT
UPPER(concat(COALESCE(INVDTL.dtlnum::VARCHAR,''),'~','JDAWMSASIA')) as WHSE_TRNSFR_ORD_LN_KEY,
'JDAWMS' AS {{var('column_srcsyskey')}},
MD5(UPPER(concat(COALESCE(INVDTL.dtlnum::VARCHAR,''),'~','JDAWMSASIA'))) AS {{var('column_redhashkey')}},
{{var('default_n')}} AS {{var('column_delfromsrcflag')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
INVDTL.LOADDTS AS {{var('column_z3loddtm')}},
INVDTL.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
'JDAWMSASIA' as WMS_SCHEMA_NAME,
INVDTL.DTLNUM AS DTL_NBR,
INVDTL.SUBNUM AS SUB_INVTY_NBR,
case when length(trim(UPPER((INVDTL.PRTNUM)))) <1 then {{var('default_mapkey')}} when UPPER(INVDTL.PRTNUM)
IS NULL then {{var('default_mapkey')}} else UPPER(INVDTL.PRTNUM) end AS PROD_KEY,
--INVDTL.PRTNUM AS PROD_ID,
--case when length(trim(UPPER((INVDTL.ORGCOD)))) <1 then {{var('default_mapkey')}} when UPPER(INVDTL.ORGCOD)
--IS NULL then {{var('default_mapkey')}} else UPPER(INVDTL.ORGCOD) end AS SRC_LOC_KEY,
INVDTL.ORGCOD AS PROD_ORIG_CD,
INVDTL.REVLVL AS TRK_DETAILS_TEXT,
TRY_TO_TIMESTAMP(replace(INVDTL.FIFDTE,'"','')) AS FIFO_CALC_DTE,
TRY_TO_TIMESTAMP(replace(INVDTL.MANDTE,'"','')) AS PROD_MFG_DTE,
INVDTL.UNTQTY AS INVTY_STK_KEEP_QTY,
INVDTL.UNTCAS AS INVTY_CASE_QTY,
INVDTL.UNTPAK AS INVTY_INNER_PKG_QTY,
INVDTL.FTPCOD AS WHSE_PART_NBR,
INVDTL.RCVKEY AS INVTY_RCVR_ID,
case when length(trim(UPPER((INVDTL.SHIP_LINE_ID)))) <1 then {{var('default_mapkey')}} when UPPER(INVDTL.SHIP_LINE_ID)
IS NULL then {{var('default_mapkey')}} when TRIM(UPPER(INVDTL.SHIP_LINE_ID)) ='NULL' then {{var('default_mapkey')}} else UPPER(INVDTL.SHIP_LINE_ID) end AS SHIP_LN_ID_LKEY,
INVDTL.WRKREF AS PICK_INVTY_REF_NBR,
INVDTL.WRKREF_DTL AS PICK_INVTY_DTL_NBR,
TRY_TO_TIMESTAMP(replace(INVDTL.ADDDTE,'"','')) AS PROD_ADD_DTE,
TRY_TO_TIMESTAMP(replace(INVDTL.RCVDTE,'"','')) AS PROD_RCV_DTE,
TRY_TO_TIMESTAMP(replace(INVDTL.LSTMOV,'"','')) AS PROD_LAST_MVMNT_DTE,
TRY_TO_TIMESTAMP(replace(INVDTL.LSTDTE,'"','')) AS PROD_LAST_ACTVTY_DTE,
case when length(trim(UPPER((INVDTL.LSTCOD)))) <1 then {{var('default_mapkey')}} when UPPER(INVDTL.LSTCOD)
IS NULL then {{var('default_mapkey')}} else UPPER(INVDTL.LSTCOD) end AS LAST_ACTVTY_TYP_LKEY,
INVDTL.CATCH_QTY AS CATCH_STAT_QTY,
case when length(trim(UPPER((INVDTL.LST_ARECOD)))) <1 then {{var('default_mapkey')}} when UPPER(INVDTL.LST_ARECOD)
IS NULL then {{var('default_mapkey')}} else UPPER(INVDTL.LST_ARECOD) end AS INVTY_PART_CD_LKEY
FROM {{source('JDAWMSASIA','INVDTL')}} INVDTL