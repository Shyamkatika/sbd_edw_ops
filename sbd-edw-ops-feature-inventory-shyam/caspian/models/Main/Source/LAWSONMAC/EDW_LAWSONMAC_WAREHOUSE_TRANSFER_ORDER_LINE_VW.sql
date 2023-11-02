SELECT
CONCAT(coalesce(WMTRNXP.TRDIV::varchar,''),'~',coalesce(WMTRNXP.TRTCDE::varchar,''),'~',coalesce(WMTRNXP.TRITEM::varchar,''),'~',
coalesce(WMTRNXP.TRTSEQ::varchar,''),'~',coalesce(WMTRNXP.TRFWHS::varchar,''),'~',coalesce(WMTRNXP.TRRORD::varchar,''),'~',coalesce(WMTRNXP.TRDTRF::varchar,'')
,'~',coalesce(WMTRNXP.TRTIME::varchar,''),'~',coalesce(WMTRNXP.TRWSID::varchar,''),'~',coalesce(WMTRNXP.TRUSER::varchar,'')) AS WHSE_TRNSFR_ORD_LN_KEY,

'LAWSONMAC' AS {{var('column_srcsyskey')}},

MD5(CONCAT(coalesce(WMTRNXP.TRDIV::varchar,''),'~',coalesce(WMTRNXP.TRTCDE::varchar,''),'~',coalesce(WMTRNXP.TRITEM::varchar,''),'~',
coalesce(WMTRNXP.TRTSEQ::varchar,''),'~',coalesce(WMTRNXP.TRFWHS::varchar,''),'~',coalesce(WMTRNXP.TRRORD::varchar,''),'~',coalesce(WMTRNXP.TRDTRF::varchar,'')
,'~',coalesce(WMTRNXP.TRTIME::varchar,''),'~',coalesce(WMTRNXP.TRWSID::varchar,''),'~',coalesce(WMTRNXP.TRUSER::varchar,''))) AS {{var('column_rechashkey')}},

{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
WMTRNXP.LOADDTS AS {{var('column_z3loddtm')}},
WMTRNXP.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},

--case when length(trim(UPPER((WMTRNXP.TRITEM)))) <1 then {{var('default_mapkey')}} when UPPER(WMTRNXP.TRITEM)
--IS NULL then {{var('default_mapkey')}} else UPPER(WMTRNXP.TRITEM) end AS PROD_KEY,

UPPER(CONCAT(COALESCE(WMTRNXP.TRDIV,''), '~',COALESCE(WMTRNXP.TRITEM,''))) AS PROD_KEY,

WMTRNXP.TRLOCB AS WHSE_LOC_ID,

WMTRNXP.TRCCDE AS CNTRY_CD,

WMTRNXP.TRCCD2 AS ALT_CNTRY_CD,

WMTRNXP.TRFLOC AS SRC_LOC_ID,

WMTRNXP.TRTLOC AS TGT_LOC_ID,

case when length(trim(UPPER((WMTRNXP.TRTCDE)))) <1 then {{var('default_mapkey')}} when UPPER(WMTRNXP.TRTCDE)
IS NULL then {{var('default_mapkey')}} else UPPER(WMTRNXP.TRTCDE) end AS TXN_TYP_LKEY,

case when length(trim(UPPER((WMTRNXP.TRPL)))) <1 then {{var('default_mapkey')}} when UPPER(WMTRNXP.TRPL)
IS NULL then {{var('default_mapkey')}} else UPPER(WMTRNXP.TRPL) end AS PROD_LN_TYP_LKEY,

case when length(trim(UPPER((WMTRNXP.TRTYP)))) <1 then {{var('default_mapkey')}} when UPPER(WMTRNXP.TRTYP)
IS NULL then {{var('default_mapkey')}} else UPPER(WMTRNXP.TRTYP) end AS ADJ_TYP_LKEY,

case when length(trim(UPPER((WMTRNXP.TRSTS)))) <1 then {{var('default_mapkey')}} when UPPER(WMTRNXP.TRSTS)
IS NULL then {{var('default_mapkey')}} else UPPER(WMTRNXP.TRSTS) end AS TXN_STAT_LKEY,


WMTRNXP.TRDIV as DIV_NBR,
WMTRNXP.TRISEQ as PROD_SEQ_NBR,
WMTRNXP.TRPQTY as TRNSFR_QTY,
WMTRNXP.TROQTY as ORD_QTY,
WMTRNXP.TRTQTY as TOT_RCV_QTY,
WMTRNXP.TRBBQ as STARTING_BIN_QTY,
WMTRNXP.TRQTY1 as MISC_1_QTY,
WMTRNXP.TRQTY2 as MISC_2_QTY,
WMTRNXP.TRQTY3 as MISC_3_QTY,
WMTRNXP.TRQTY4 as MISC_4_QTY,
WMTRNXP.TRFWHS as SRC_WHSE_NAME,
WMTRNXP.TRTWHS as TGT_WHSE_NAME,
WMTRNXP.TRFLT as SRC_SUB_LOC_TYP,
WMTRNXP.TRTLT as TGT_SUB_LOC_TYP,
WMTRNXP.TRSTD1 as STD_1_COST,
WMTRNXP.TRSTD2 as STD_2_COST,
WMTRNXP.TRSTD3 as STD_3_COST,
WMTRNXP.TRSTD4 as STD_4_COST,
WMTRNXP.TRSTD5 as STD_5_COST,
WMTRNXP.TRPRCE as PROD_PRC,
WMTRNXP.TRSTAT as TXN_STAT_TYP,
WMTRNXP.TRMKCT as PROD_MKT_CTGY_TYP,
WMTRNXP.TRMKCL as PROD_MKT_CLS_TYP,
WMTRNXP.TRMKFM as PROD_MKT_FAMILY_TYP,
WMTRNXP.TRPGM as PROD_PGM_NAME,
WMTRNXP.TRWSID as WORKSTATION_NBR,
WMTRNXP.TRTCNY as TXN_YR,
WMTRNXP.TRTIME as TXN_TM,
WMTRNXP.TRTSEQ as TXN_SEQ_NBR,
WMTRNXP.TRYRWK as TXN_YR_WK,
WMTRNXP.TRWCNY as TXN_YR_WITH_WK,
WMTRNXP.TRYRMO as ACCTG_MTH,
WMTRNXP.TRMCNY as ACCTG_YR,
WMTRNXP.TRGL as GL_ACCT_NBR,
WMTRNXP.TRRORD as TGT_ORD_NBR,
WMTRNXP.TRVEND as VEND_ID,
WMTRNXP.TRBAC as BUYER_NAME,
WMTRNXP.TRANLC as ANALYST_CD,
WMTRNXP.TRRCPT as RCPT_SEQ_NBR,
WMTRNXP.TRPRTF as PRINT_TYP,
WMTRNXP.TRCOMF as TXN_CMPLT_TYP,
WMTRNXP.TRALF1 as MISC_1_FIELD,
WMTRNXP.TRALF4 as MISC_2_FIELD,
WMTRNXP.TRCNY1 as PROD_RCV_YR,
WMTRNXP.TRRCNY as TGT_RCV_YR,
WMTRNXP.TRPUL as PULL_SEQ_NBR,
WMTRNXP.TRUSER as TXN_USER_ID,

TRY_TO_DATE(REPLACE(ROUND(TRDTRF),substr(ROUND(TRDTRF),1,2),round(substr(ROUND(TRDTRF),1,2)+1928)),'YYYYMMDD') as  TXN_DTE,
TRY_TO_DATE(REPLACE(ROUND(TRDTE1),substr(ROUND(TRDTE1),1,2),round(substr(ROUND(TRDTE1),1,2)+1928)),'YYYYMMDD') as  PROD_RCV_DTE,
TRY_TO_DATE(REPLACE(ROUND(TRDATR),substr(ROUND(TRDATR),1,2),round(substr(ROUND(TRDATR),1,2)+1928)),'YYYYMMDD') as  TGT_RCV_DTE

FROM {{source('LAWSONMAC','WMTRNXP')}} WMTRNXP