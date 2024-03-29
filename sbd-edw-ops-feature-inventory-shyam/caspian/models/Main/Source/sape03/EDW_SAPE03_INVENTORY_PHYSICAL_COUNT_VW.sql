SELECT
UPPER(concat(COALESCE(IKPF.IBLNR,''),'~',COALESCE(IKPF.GJAHR,''),'~',COALESCE(ISEG.ZEILI,''))) AS PHY_INVTY_KEY,
'SAPE03' AS {{var('column_srcsyskey')}},
MD5(UPPER(concat(COALESCE(IKPF.IBLNR,''),'~',COALESCE(IKPF.GJAHR,''),'~',COALESCE(ISEG.ZEILI,'')))) AS {{var('column_redhashkey')}},
{{var('default_n')}} AS {{var('column_delfromsrcflag')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
IKPF.LOADDTS AS {{var('column_z3loddtm')}},
IKPF.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
{{var('default_key')}} AS ORG_KEY,
TRY_TO_DATE(IKPF.BLDAT,'YYYYMMDD') as PHY_INVTY_ISS_DTE,
case when UPPER(IKPF.DSTAT) = 'X' THEN 'Y' ELSE 'N' END AS PHY_INVTY_ADJ_POST_STAT_FLAG,
TRY_TO_DATE(IKPF.GIDAT,'YYYYMMDD') as PHY_INVTY_CNT_PLND_DTE,
IKPF.IBLTXT AS PHY_INVTY_DOC_DESC,
case when length(trim(UPPER((IKPF.INVART)))) <1 then {{var('default_mapkey')}} when UPPER(IKPF.INVART)
IS NULL then {{var('default_mapkey')}} else UPPER(IKPF.INVART) end AS PHY_INVTY_CNT_TYP_LKEY,
IKPF.INVNU AS PHY_INVTY_NBR,
--case when length(trim(UPPER((IKPF.LGORT)))) <1 then {{var('default_mapkey')}} when UPPER(IKPF.LGORT)
--IS NULL then {{var('default_mapkey')}} else UPPER(IKPF.LGORT) end AS STORG_SUB_LOC_KEY,
IKPF.LGORT AS SUB_SLOC_ID,
IKPF.SOBKZ  AS SPCL_STK_TYP,
case when length(trim(UPPER((IKPF.WERKS)))) <1 then {{var('default_mapkey')}} when UPPER(IKPF.WERKS)
IS NULL then {{var('default_mapkey')}} else UPPER(IKPF.WERKS) end AS LOC_KEY,
IKPF.XBLNI AS PHY_INVTY_REF_NBR,
case when  UPPER(IKPF.ZSTAT) = 'X' THEN 'Y' ELSE 'N' END AS CNT_STAT_FLAG,
IKPF.IBLNR AS PHY_INVTY_DOC_NBR,
TRY_TO_DATE(ISEG.AEDAT,'YYYYMMDD') as LN_ITEM_MOD_DTE,
case when length(trim(UPPER((ISEG.ATTYP)))) <1 then {{var('default_mapkey')}} when UPPER(ISEG.ATTYP)
IS NULL then {{var('default_mapkey')}} else UPPER(ISEG.ATTYP) end AS MATL_CTGY_TYP_LKEY,
case when length(trim(UPPER((ISEG.BSTAR)))) <1 then {{var('default_mapkey')}} when UPPER(ISEG.BSTAR)
IS NULL then {{var('default_mapkey')}} else UPPER(ISEG.BSTAR) end AS STK_TYP_LKEY,
ISEG.BUCHM AS BOOK_INVTY_QTY,
ISEG.CHARG AS PROD_BATCH_NBR,
ISEG.DIWZL AS INVTY_DIFF_CNT,
ISEG.DMBTR AS LCRNCY_DIFF_AMT,
case when length(trim(UPPER((ISEG.ERFME)))) <1 then {{var('default_mapkey')}} when UPPER(ISEG.ERFME)
IS NULL then {{var('default_mapkey')}} else UPPER(ISEG.ERFME) end AS  CNT_ENTR_UOM_KEY,
ISEG.ERFMG AS CNT_ENTR_UOM_QTY,
ISEG.EXVKW AS LCRNCY_EXTNL_SLS_AMT,
--TRY_TO_DATE(ISEG.GJAHR,'YYYYMMDD') as PHY_INVTY_DOC_FYR_DTE,
case when length(trim(UPPER((ISEG.GRUND)))) <1 then {{var('default_mapkey')}} when UPPER(ISEG.GRUND)
IS NULL then {{var('default_mapkey')}} else UPPER(ISEG.GRUND) end AS INVTY_DIFF_RESN_LKEY,
--ISEG.IBLNR AS PHY_INVTY_DOC_NBR,
ISEG.KDAUF AS SLS_ORD_NBR,
ISEG.KDEIN AS SLS_ORD_DLV_SCHED_NBR,
ISEG.KDPOS AS SLS_ORD_ITEM_NBR,
ISEG.KUNNR AS CUST_ACCT_NBR,
case when length(trim(UPPER((ISEG.KWART)))) <1 then {{var('default_mapkey')}} when UPPER(ISEG.KWART)
IS NULL then {{var('default_mapkey')}} else UPPER(ISEG.KWART) end AS VAL_ONLY_PROD_LKEY,
--case when length(trim(UPPER((ISEG.LIFNR)))) <1 then {{var('default_mapkey')}} when UPPER(ISEG.LIFNR)
--IS NULL then {{var('default_mapkey')}} else UPPER(ISEG.LIFNR) end AS VEND_KEY,
ISEG.LIFNR AS VEND_ID,
case when length(trim(UPPER((ISEG.MATNR)))) <1 then {{var('default_mapkey')}} when UPPER(ISEG.MATNR)
IS NULL then {{var('default_mapkey')}} else UPPER(ISEG.MATNR) end AS PROD_KEY,
ISEG.MBLNR AS PROD_DOC_NBR,
case when length(trim(UPPER((ISEG.MEINS)))) <1 then {{var('default_mapkey')}} when UPPER(ISEG.MEINS)
IS NULL then {{var('default_mapkey')}} else UPPER(ISEG.MEINS) end AS BASE_UOM_KEY,
ISEG.MENGE AS PROD_PHY_INVTY_QTY,
ISEG.MJAHR as PROD_DOC_YR,
ISEG.NBLNR AS RECOUNT_DOC_NBR,
ISEG.PLPLA AS STORG_BIN_NBR,
 ISEG.PS_PSP_PNR  AS WORK_BRKDN_STRC_NBR,
case when length(trim(UPPER((ISEG.SAMAT)))) <1 then {{var('default_mapkey')}} when UPPER(ISEG.SAMAT)
IS NULL then {{var('default_mapkey')}} else UPPER(ISEG.SAMAT) end AS CROSS_PLANT_CONFIGURABLE_PROD_KEY,
--case when length(trim(UPPER((ISEG.SOBKZ)))) <1 then {{var('default_mapkey')}} when UPPER(ISEG.SOBKZ)
--IS NULL then {{var('default_mapkey')}} else UPPER(ISEG.SOBKZ) end AS SPCL_STK_LKEY,
ISEG.USNAD AS ADJ_POST_BY_USERID,
ISEG.USNAZ AS COUNTED_BY_USERID,
ISEG.VKMZL AS INVTY_DIFF_WITH_VAT_SLS_AMT,
ISEG.VKNZL AS INVTY_DIFF_SLS_AMT,
ISEG.VKWRA AS SLS_PRC,
ISEG.VKWRT AS SLS_PRC_WITH_VAT_AMT,
case when length(trim(UPPER((ISEG.WAERS)))) <1 then {{var('default_mapkey')}} when UPPER(ISEG.WAERS)
IS NULL then {{var('default_mapkey')}} else UPPER(ISEG.WAERS) end AS CRNCY_KEY,
ISEG.WRTBM AS BOOK_QTY_VAL_AMT,
ISEG.WRTZL AS CNT_VAL_AMT,
TRY_TO_DATE(ISEG.WSTI_COUNTDATE,'YYYYMMDD') as CNT_OPN_DTE,
TRY_TO_DATE(ISEG.WSTI_FREEZEDATE,'YYYYMMDD') as INVTY_BAL_FREEZE_DTE,
try_to_timestamp(ISEG.WSTI_FREEZETIME) AS INVTY_BAL_FREEZE_TM,
ISEG.WSTI_POSM AS BASE_UOM_BOOK_CHG_QTY,
ISEG.WSTI_POSW AS RETL_BOOK_CHG_VAL_AMT,
try_to_timestamp(ISEG.WSTI_XCALC) AS BOOK_INVTY_CALC_TM,
CASE WHEN ISEG.XAMEI = 'X' THEN 'Y' ELSE 'N' END AS IS_BASE_UOM_FLAG,
CASE WHEN ISEG.XDIFF = 'X' THEN 'Y' ELSE 'N' END AS DIFF_POST_FLAG,
CASE WHEN ISEG.XDISPATCH = 'X' THEN 'Y' ELSE 'N' END AS CNSMPTN_DIST_FLAG,
CASE WHEN ISEG.XLOEK = 'X' THEN 'Y' ELSE 'N' END AS ITEM_DEL_FLAG,
CASE WHEN ISEG.XNULL = 'X' THEN 'Y' ELSE 'N' END AS ZERO_CNT_FLAG,
CASE WHEN ISEG.XNZAE = 'X' THEN 'Y' ELSE 'N' END AS ITEM_RECOUNT_FLAG,
CASE WHEN ISEG.XZAEL = 'X' THEN 'Y' ELSE 'N' END AS ITEM_COUNTED_FLAG,
ISEG.ZEILE AS LN_ITEM_PROD_NBR,
ISEG.ZEILI AS LN_ITEM_NBR,
TRY_TO_TIMESTAMP_NTZ(CONCAT(TRY_TO_DATE(ISEG.WSTI_COUNTDATE,'YYYYMMDD'),' ',

            TRY_TO_TIME((case when length(trim(ISEG.WSTI_COUNTTIME)) <1 then '000000'

when ISEG.WSTI_COUNTTIME IS NULL then '000000'

else ISEG.WSTI_COUNTTIME end),'HH24MISS'))) AS CNT_OPN_TM,
IKPF.KEORD AS GRP_CRITERION_TYP,
CASE WHEN IKPF.LSTAT = 'X' THEN 'Y' ELSE 'N' END AS DOC_ITEM_DEL_FLAG,
IKPF.MONAT AS DOC_CREATE_NBR,
IKPF.ORDNG AS GRP_CRITERION_NBR,
IKPF.SPERR AS POST_BLOCK_DUE_INDCTR,
CASE WHEN IKPF.XBUFI = 'X' THEN 'Y' ELSE 'N' END AS FREEZE_BOOK_INVTY_FLAG,
TRY_TO_DATE(IKPF.BUDAT,'YYYYMMDD') as DOC_POST_DTE,
ISEG.ABCIN AS CYCLE_CNT_INDCTR,
--TRY_TO_DATE(ISEG.BUDAT,'YYYYMMDD') as DOC_POST_DTE,
--ISEG.ZLDAT AS LAST_INVTY_DTE_CNT,
IKPF.GJAHR AS PHY_INVTY_DOC_FYR,
TRY_TO_DATE(IKPF.ZLDAT,'YYYYMMDD') AS LAST_INVTY_DTE,
--case when length(trim(UPPER((ISEG.ZEILE)))) <1 then {{var('default_mapkey')}} when UPPER(ISEG.ZEILE)
--IS NULL then {{var('default_mapkey')}} else UPPER(ISEG.ZEILE) end AS LOC_KEY,
--ase when length(trim(UPPER((ISEG.LGORT )))) <1 then {{var('default_mapkey')}} when UPPER(ISEG.LGORT )
--IS NULL then {{var('default_mapkey')}} else UPPER(ISEG.LGORT ) end AS STORG_SUB_LOC_KEY,
--ISEG.XBLNI AS PHY_INVTY_REF_NBR,
IKPF.VGART AS TXN_TYP,
IKPF.WSTI_BSTAT AS INVTY_BAL_CALC_STAT,
ISEG.BUCHW AS SLS_PRC_BOOK_VAL_AMT
FROM {{source('SAPE03','IKPF')}}  IKPF 
LEFT JOIN {{source('SAPE03', 'ISEG')}} ISEG ON IKPF.IBLNR = ISEG.IBLNR AND IKPF.GJAHR = ISEG.GJAHR
WHERE IKPF.MANDT={{var('sape03mandtftr')}}