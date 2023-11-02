SELECT 
'SAPP10' AS {{var('column_srcsyskey')}},
UPPER(CONCAT(coalesce(MARDH.MATNR::VARCHAR,''),'~',coalesce(MARDH.WERKS::VARCHAR,'')
,'~',coalesce(MARDH.LGORT::VARCHAR,''),'~',coalesce(MARDH.LFGJA::VARCHAR,''),'~',coalesce(MARDH.LFMON::VARCHAR,''))) as INVTY_ON_HAND_SNAPSHOT_KEY,
MD5(UPPER(CONCAT(coalesce(MARDH.MATNR::VARCHAR,''),'~',coalesce(MARDH.WERKS::VARCHAR,'')
,'~',coalesce(MARDH.LGORT::VARCHAR,''),'~',coalesce(MARDH.LFGJA::VARCHAR,''),'~',coalesce(MARDH.LFMON::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
MARDH.LOADDTS AS {{var('column_z3loddtm')}},
MARDH.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
MARDH.LGORT  AS SLOC_ID,
case when length(trim(UPPER((MARDH.MATNR)))) <1 then {{var('default_mapkey')}} when UPPER(MARDH.MATNR)
IS NULL then {{var('default_mapkey')}} else UPPER(MARDH.MATNR) end AS PROD_KEY,
case when length(trim(UPPER((MARDH.WERKS)))) <1 then {{var('default_mapkey')}} when UPPER(MARDH.WERKS)
IS NULL then {{var('default_mapkey')}} else UPPER(MARDH.WERKS) end AS LOC_KEY,
MARDH.EINME	as	PAST_RESTRCT_BATCHES_TOT_QTY	,
MARDH.INSME	as	PAST_INSPCT_STK_QTY	,
MARDH.LABST	as	PAST_VALUATED_STK_QTY	,
MARDH.LFGJA	as	PAST_FYR_TM	,
MARDH.LFMON	as	PAST_FSCL_PERD_TM	,
MARDH.RETME	as	PAST_BLKD_STK_RTRN_QTY	,
MARDH.SPEME	as	PAST_BLKD_STK_QTY	,
MARDH.UMLME	as	PAST_STK_IN_TRNSFR_QTY	,
MARDH.VKLAB	as	PAST_PROD_STK_VAL	,
MARDH.VKUML	as	PAST_SLS_VAL
FROM {{source('SAPP10','MARDH')}} MARDH
  WHERE MARDH.MANDT={{var('sapp10mandtftr')}}