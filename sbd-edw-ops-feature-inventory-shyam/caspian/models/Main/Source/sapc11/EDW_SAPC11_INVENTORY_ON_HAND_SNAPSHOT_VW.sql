SELECT 
'SAPC11' AS {{var('column_srcsyskey')}},
UPPER(CONCAT(coalesce(MARD.MATNR::VARCHAR,''),'~',coalesce(MARD.WERKS::VARCHAR,'')
,'~',coalesce(MARD.LGORT::VARCHAR,''),'~',coalesce(MARD.LFGJA::VARCHAR,''),'~',coalesce(MARD.LFMON::VARCHAR,''))) as INVTY_ON_HAND_SNAPSHOT_KEY,
MD5(UPPER(CONCAT(coalesce(MARD.MATNR::VARCHAR,''),'~',coalesce(MARD.WERKS::VARCHAR,'')
,'~',coalesce(MARD.LGORT::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
MARD.LOADDTS AS {{var('column_z3loddtm')}},
MARD.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
case when length(trim(UPPER((MARD.BSKRF)))) <1 then {{var('default_mapkey')}} when UPPER(MARD.BSKRF)
IS NULL then {{var('default_mapkey')}} else UPPER(MARD.BSKRF) end AS INVTY_CRCTD_FACTOR_LKEY,
case when length(trim(UPPER((MARD.DISKZ)))) <1 then {{var('default_mapkey')}} when UPPER(MARD.DISKZ)
IS NULL then {{var('default_mapkey')}} else UPPER(MARD.DISKZ) end AS SLOC_MRP_LKEY,
TRY_TO_DATE(MARD.DLINL,'YYYYMMDD')	as	UNRSTR_USE_STK_LAST_CNT_DTE	,
MARD.EINME	as	RESTRCT_BATCHES_TOT_QTY	,
MARD.HERKL	as	ORIG_CNTRY_ID	,
MARD.INSME	as	QLTY_INSPCT_STK_QTY	,
MARD.KEINM	as	RESTRCT_USE_STK_QTY	,
MARD.KINSM	as	QLTY_INSPCT_CNSGN_STK_QTY	,
MARD.KLABS	as	UNRSTR_USE_STK_QTY	,
MARD.KSPEM	as	BLKD_CNSGN_STK_QTY	,
case when length(trim(UPPER((MARD.KZILE)))) <1 then {{var('default_mapkey')}} when UPPER(MARD.KZILE)
IS NULL then {{var('default_mapkey')}} else UPPER(MARD.KZILE) end AS INVTY_RESTRCT_USE_STK_STAT_LKEY,
--MARD.KZILE	as	INVTY_RESTRCT_USE_STK_STAT_LKEY	,
case when length(trim(UPPER((MARD.KZILL)))) <1 then {{var('default_mapkey')}} when UPPER(MARD.KZILL)
IS NULL then {{var('default_mapkey')}} else UPPER(MARD.KZILL) end AS INVTY_WHSE_USE_STK_STAT_LKEY,
--MARD.KZILL	as	INVTY_WHSE_USE_STK_STAT_LKEY	,
case when length(trim(UPPER((MARD.KZILQ)))) <1 then {{var('default_mapkey')}} when UPPER(MARD.KZILQ)
IS NULL then {{var('default_mapkey')}} else UPPER(MARD.KZILQ) end AS INVTY_QLTY_INSPCT_STK_STAT_LKEY,
---MARD.KZILQ	as	INVTY_QLTY_INSPCT_STK_STAT_LKEY	,
case when length(trim(UPPER((MARD.KZILS)))) <1 then {{var('default_mapkey')}} when UPPER(MARD.KZILS)
IS NULL then {{var('default_mapkey')}} else UPPER(MARD.KZILS) end AS INVTY_BLKD_STK_STAT_LKEY,
---MARD.KZILS	as	INVTY_BLKD_STK_STAT_LKEY	,
case when length(trim(UPPER((MARD.KZVLE)))) <1 then {{var('default_mapkey')}} when UPPER(MARD.KZVLE)
IS NULL then {{var('default_mapkey')}} else UPPER(MARD.KZVLE) end AS PAST_INVTY_RESTRCT_USE_STK_STAT_LKEY,
---MARD.KZVLE	as	PAST_INVTY_RESTRCT_USE_STK_STAT_LKEY	,
case when length(trim(UPPER((MARD.KZVLL)))) <1 then {{var('default_mapkey')}} when UPPER(MARD.KZVLL)
IS NULL then {{var('default_mapkey')}} else UPPER(MARD.KZVLL) end AS PAST_INVTY_WHSE_USE_STK_STAT_LKEY,
---MARD.KZVLL	as	PAST_INVTY_WHSE_USE_STK_STAT_LKEY	,
case when length(trim(UPPER((MARD.KZVLQ)))) <1 then {{var('default_mapkey')}} when UPPER(MARD.KZVLQ)
IS NULL then {{var('default_mapkey')}} else UPPER(MARD.KZVLQ) end AS PAST_INVTY_QLTY_INSPCT_STK_STAT_LKEY,
---MARD.KZVLQ	as	PAST_INVTY_QLTY_INSPCT_STK_STAT_LKEY	,
case when length(trim(UPPER((MARD.KZVLS)))) <1 then {{var('default_mapkey')}} when UPPER(MARD.KZVLS)
IS NULL then {{var('default_mapkey')}} else UPPER(MARD.KZVLS) end AS PAST_INVTY_BLKD_STK_STAT_LKEY,
---MARD.KZVLS	as	PAST_INVTY_BLKD_STK_STAT_LKEY	,
MARD.LABST	as	VALUATED_UNRSTR_STK_QTY	,
MARD.LBSTF	as	REPL_QTY	,
MARD.LFGJA	as	CURR_FYR	,
MARD.LFMON	as	CURR_CLNDR_PERD_NBR	,
MARD.LGORT  AS SLOC_ID,
MARD.LGPBE	as	STORG_BIN_ID	,
MARD.LMINB	as	REORD_POINT_LVL_QTY	,
case when length(trim(UPPER((MARD.LSOBS)))) <1 then {{var('default_mapkey')}} when UPPER(MARD.LSOBS)
IS NULL then {{var('default_mapkey')}} else UPPER(MARD.LSOBS) end AS SPCL_PROCUR_TYP_LKEY,
---MARD.LSOBS	as	SPCL_PROCUR_TYP_LKEY	,
case  when UPPER(MARD.LVORM) ='X' then 'Y' else 'N'  end AS SLOC_LVL_DEL_FLAG,
MARD.LWMKB	as	PICK_AREA_FOR_LEAN_WM_TYP	,
case when length(trim(UPPER((MARD.MATNR)))) <1 then {{var('default_mapkey')}} when UPPER(MARD.MATNR)
IS NULL then {{var('default_mapkey')}} else UPPER(MARD.MATNR) end AS PROD_KEY,
MARD.MDJIN	as	PHY_INVTY_FYR	,
case  when UPPER(MARD.MDRUE) ='X' then 'Y'  else 'N'  end	as	PAST_RCRD_EXISTS_FLAG	,
MARD.PRCTL	as	FIN_PROFIT_CNTR_ID	,
MARD.PSTAT	as	MTNC_STAT_TYP_LKEY	,
MARD.RETME	as	BLKD_STK_RTRN_QTY	,
MARD.SPEME	as	BLKD_STK_QTY	,
MARD.SPERR	as	PHY_INVTY_BLOCK_TYP	,
MARD.UMLME	as	TRANSFERRED_STK_QTY	,
MARD.VKLAB	as	VALUATED_STK_SLS_VAL_AMT	,
MARD.VKUML	as	STK_TRNSFR_SLS_VAL_AMT	,
MARD.VMEIN	as	PAST_PERD_RESTRCT_STK_QTY	,
MARD.VMINS	as	PAST_PERD_QLTY_INSPCT_QTY	,
MARD.VMLAB	as	PAST_PERD_UNRSTR_USE_STK_QTY	,
MARD.VMRET	as	PAST_PERD_BLKD_STK_RTRN_QTY	,
MARD.VMSPE	as	PAST_PERD_BLKD_STK_QTY	,
MARD.VMUML	as	PAST_PERD_TRNSFR_STK_QTY	,
case when length(trim(UPPER((MARD.WERKS)))) <1 then {{var('default_mapkey')}} when UPPER(MARD.WERKS)
IS NULL then {{var('default_mapkey')}} else UPPER(MARD.WERKS) end AS LOC_KEY
FROM {{source('SAPC11','MARD')}} MARD
  WHERE MARD.MANDT={{var('sapc11mandtftr')}}
