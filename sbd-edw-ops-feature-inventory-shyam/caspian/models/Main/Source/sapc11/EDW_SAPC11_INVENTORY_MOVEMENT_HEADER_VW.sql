SELECT
'SAPC11' AS {{var('column_srcsyskey')}},
UPPER(CONCAT(coalesce(MKPF.MBLNR::VARCHAR,''),'~',coalesce(MKPF.MJAHR::VARCHAR,''))) 
as INVTY_MVMNT_HDR_KEY,
MD5(UPPER(CONCAT(coalesce(MKPF.MBLNR::VARCHAR,''),'~',coalesce(MKPF.MJAHR::VARCHAR,''))))
AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
MKPF.LOADDTS AS {{var('column_z3loddtm')}},
MKPF.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
MKPF.MBLNR	as	PROD_MVMNT_DOC_NBR	,
MKPF.MJAHR	as	PROD_MVMNT_DOC_YR	,

case when length(trim(UPPER((MKPF.VGART)))) <1 then {{var('default_mapkey')}} when UPPER(MKPF.VGART)
IS NULL then {{var('default_mapkey')}} else UPPER(MKPF.VGART) end AS MVMNT_TYP_LKEY,

case when length(trim(UPPER((MKPF.BLART)))) <1 then {{var('default_mapkey')}} when UPPER(MKPF.BLART)
IS NULL then {{var('default_mapkey')}} else UPPER(MKPF.BLART) end AS ACCT_DOC_TYP_LKEY,
case when length(trim(UPPER((MKPF.BLAUM)))) <1 then {{var('default_mapkey')}} when UPPER(MKPF.BLAUM)
IS NULL then {{var('default_mapkey')}} else UPPER(MKPF.BLAUM) end AS ACCT_DOC_SUB_TYP_LKEY,
TRY_TO_DATE(MKPF.BLDAT,'YYYYMMDD')	as	PROD_MVMNT_DOC_DTE	,
TRY_TO_DATE(MKPF.BUDAT,'YYYYMMDD')	as	PROD_MVMNT_POST_DTE	,
TRY_TO_DATE(MKPF.CPUDT,'YYYYMMDD')	as	PROD_MVMNT_DOC_ENTR_DTE	,
TO_TIME(MKPF.CPUTM,'HH24MISS') as PROD_MVMNT_DOC_ENTR_TM,
TRY_TO_DATE(MKPF.AEDAT,'YYYYMMDD')	as	PROD_MVMNT_DOC_CHG_DTE	,
MKPF.USNAM	as	PROD_MVMNT_DOC_UPD_USER_NAME	,
MKPF.XBLNR	as	PRECEDING_DOC_NBR	,
MKPF.BKTXT	as	PROD_MVMNT_DOC_HDR_TEXT	,
MKPF.FRATH	as	UNPLANNED_DLV_COST	,
MKPF.FRBNR	as	BOL_NBR	,
case when length(trim(UPPER((MKPF.WEVER)))) <1 then {{var('default_mapkey')}} when UPPER(MKPF.WEVER)
IS NULL then {{var('default_mapkey')}} else UPPER(MKPF.WEVER) end AS GR_SLIP_VER_LKEY,
MKPF.XABLN	as	GOODS_RCPT_NBR	,
MKPF.BLA2D	as	ADDTN_ACCT_DOC_NBR	,
MKPF.TCODE2	as	PROD_MVMNT_DOC_CREATE_PROC_TEXT	,
case when length(trim(UPPER((MKPF.BFWMS)))) <1 then {{var('default_mapkey')}} when UPPER(MKPF.BFWMS)
IS NULL then {{var('default_mapkey')}} else UPPER(MKPF.BFWMS) end AS CTRL_POST_FOR_EXTNL_WMS_LKEY,
MKPF.EXNUM	as	FOREIGN_TRADE_DATA_NBR	,

TO_TIME(MKPF.SPE_BUDAT_UHR,'HH24MISS') as GOODS_ISS_TM,

MKPF.SPE_BUDAT_ZONE	as	GOODS_ISS_TM_ZONE_TEXT	,
MKPF.LE_VBELN	as	DLV_DOC_NBR	,
MKPF.SPE_LOGSYS AS EWM_SYS_NAME,
MKPF.AWSYS AS GOODS_ISS_SYS_NAME,
MKPF.SPE_MDNUM_EWM	as	EXTNL_PROD_MVMNT_DOC_NBR	,
MKPF.GTS_CUSREF_NO	as	SCRAPPING_REF_NBR	,
case when UPPER(MKPF.FLS_RSTO) ='X' then'Y'else'N' end AS IS_STORE_RTRN_FLAG,
case when UPPER(MKPF.MSR_ACTIVE) ='X' then'Y'else'N' end AS IS_ADVANCE_RTRN_FLAG,
MKPF.KNUMV	as	DOC_COND_NBR,	
MKPF.TCODE	as	TXN_CD_TYP
FROM {{source('SAPC11','MKPF')}} MKPF
WHERE MKPF.MANDT={{var('sapc11mandtftr')}}