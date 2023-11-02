select
UPPER(CONCAT(coalesce(LTAK.LGNUM::VARCHAR,''),'~',coalesce(LTAK.TANUM::VARCHAR,''))) AS WHSE_TRNSFR_ORD_HDR_KEY,
'SAPC11' AS {{var('column_srcsyskey')}},
MD5(UPPER(CONCAT(coalesce(LTAK.LGNUM::VARCHAR,''),'~',coalesce(LTAK.TANUM::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
LTAK.LOADDTS AS {{var('column_z3loddtm')}},
LTAK.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},


LTAK.ZEIEI AS WHSE_TM_UNIT_DATA,


case when length(trim(UPPER((LTAK.AUSFB)))) <1 then {{var('default_mapkey')}} when UPPER(LTAK.AUSFB)
IS NULL then {{var('default_mapkey')}} else UPPER(LTAK.AUSFB) end AS PROC_CD_LKEY,

case when length(trim(UPPER((LTAK.BETYP)))) <1 then {{var('default_mapkey')}} when UPPER(LTAK.BETYP)
IS NULL then {{var('default_mapkey')}} else UPPER(LTAK.BETYP) end AS REQ_ORIG_TYP_LKEY,

case when length(trim(UPPER((LTAK.BWART)))) <1 then {{var('default_mapkey')}} when UPPER(LTAK.BWART)
IS NULL then {{var('default_mapkey')}} else UPPER(LTAK.BWART) end AS STK_MVMNT_TYP_LKEY,

case when length(trim(UPPER((LTAK.BWLVS)))) <1 then {{var('default_mapkey')}} when UPPER(LTAK.BWLVS)
IS NULL then {{var('default_mapkey')}} else UPPER(LTAK.BWLVS) end AS WHSE_MVMNT_TYP_LKEY,

case when length(trim(UPPER((LTAK.DRUKZ)))) <1 then {{var('default_mapkey')}} when UPPER(LTAK.DRUKZ)
IS NULL then {{var('default_mapkey')}} else UPPER(LTAK.DRUKZ) end AS FMT_SORT_LKEY,

case when length(trim(UPPER((LTAK.HRSTS)))) <1 then {{var('default_mapkey')}} when UPPER(LTAK.HRSTS)
IS NULL then {{var('default_mapkey')}} else UPPER(LTAK.HRSTS) end AS PERFORMANCE_STAT_LKEY,

case when length(trim(UPPER((LTAK.HUCON)))) <1 then {{var('default_mapkey')}} when UPPER(LTAK.HUCON)
IS NULL then {{var('default_mapkey')}} else UPPER(LTAK.HUCON) end AS RPT_STAT_LKEY,

case when length(trim(UPPER((LTAK.KZLEI)))) <1 then {{var('default_mapkey')}} when UPPER(LTAK.KZLEI)
IS NULL then {{var('default_mapkey')}} else UPPER(LTAK.KZLEI) end AS PERFORMANCE_RELVNT_LKEY,

case when length(trim(UPPER((LTAK.L2SKA)))) <1 then {{var('default_mapkey')}} when UPPER(LTAK.L2SKA)
IS NULL then {{var('default_mapkey')}} else UPPER(LTAK.L2SKA) end AS TWO_STEP_PICK_TYP_LKEY,

case when length(trim(UPPER((LTAK.LATER)))) <1 then {{var('default_mapkey')}} when UPPER(LTAK.LATER)
IS NULL then {{var('default_mapkey')}} else UPPER(LTAK.LATER) end AS DELAYED_OBND_DLV_LKEY,

case when length(trim(UPPER((LTAK.SPEZI)))) <1 then {{var('default_mapkey')}} when UPPER(LTAK.SPEZI)
IS NULL then {{var('default_mapkey')}} else UPPER(LTAK.SPEZI) end AS SPCL_REF_LKEY,

case when length(trim(UPPER((LTAK.TRART)))) <1 then {{var('default_mapkey')}} when UPPER(LTAK.TRART)
IS NULL then {{var('default_mapkey')}} else UPPER(LTAK.TRART) end AS TRNSFR_TYP_LKEY,

case when length(trim(UPPER((LTAK.VBTYP)))) <1 then {{var('default_mapkey')}} when UPPER(LTAK.VBTYP)
IS NULL then {{var('default_mapkey')}} else UPPER(LTAK.VBTYP) end AS REF_DOC_TYP_LKEY,


case  when UPPER(LTAK.DRUCK) ='X'  then 'Y' else 'N'  end AS ORD_IS_PRINTED_FLAG,
case  when UPPER(LTAK.KGVNQ) ='X'  then 'Y' else 'N'  end AS SEPARATE_CNFRM_POSBL_FLAG,
case  when UPPER(LTAK.KISTP) ='X'  then 'Y' else 'N'  end AS INPUT_REQ_FLAG,
case  when UPPER(LTAK.KQUIT) ='X'  then 'Y' else 'N'  end AS TRNSFR_ORD_CNFRM_FLAG,
case  when UPPER(LTAK.KVQUI) ='X'  then 'Y' else 'N'  end AS CNFRM_PROD_PICK_FLAG,
case  when UPPER(LTAK.KZPLA) ='X'  then 'Y' else 'N'  end AS PREPLANNED_TRNSFR_ORD_FLAG,
case  when UPPER(LTAK.KZTRM) ='X'  then 'Y' else 'N'  end AS TRM_TRNSFR_FLAG,
case  when UPPER(LTAK.MINWM) ='X'  then 'Y' else 'N'  end AS STORG_BIN_STK_TRNSFR_FLAG,
case  when UPPER(LTAK.MULTL) ='X'  then 'Y' else 'N'  end AS MULT_DLV_FLAG,
case  when UPPER(LTAK.PASSD) ='X'  then 'Y' else 'N'  end AS CNFRM_PASSED_TO_DLV_FLAG,
case  when UPPER(LTAK.TEILK) ='X'  then 'Y' else 'N'  end AS PRTL_SUPPLY_FLAG,


TRY_TO_DATE(LTAK.ENDAT,'YYYYMMDD') AS TRNSFR_ORD_END_DTE,
TRY_TO_DATE(LTAK.PLDAT,'YYYYMMDD') AS PLND_EXEC_DTE,
TRY_TO_DATE(LTAK.QDATU,'YYYYMMDD') AS CNFRM_DTE,
TRY_TO_DATE(LTAK.STDAT,'YYYYMMDD') AS ACT_START_DTE,

--LTAK.ENUZT AS TRNSFR_ORD_END_TM,
--LTAK.STUZT AS ACT_START_TM,


TRY_TO_TIMESTAMP_NTZ(CONCAT(TRY_TO_DATE(LTAK.ENDAT,'YYYYMMDD'),' ',
                       TRY_TO_TIME((case when length(trim(LTAK.ENUZT)) <1 then '000000'
when LTAK.ENUZT IS NULL then '000000'
else LTAK.ENUZT end),'HH24MISS'))) as TRNSFR_ORD_END_TM,


TRY_TO_TIMESTAMP_NTZ(CONCAT(TRY_TO_DATE(LTAK.STDAT,'YYYYMMDD'),' ',
                       TRY_TO_TIME((case when length(trim(LTAK.STUZT)) <1 then '000000'
when LTAK.STUZT IS NULL then '000000'
else LTAK.STUZT end),'HH24MISS'))) as ACT_START_TM,


LTAK.MJAHR as PROD_MVMNT_DOC_YR,
LTAK.KISTZ as ACT_TM_REQ_TYP,
LTAK.KZVEP as PKG_RECOMMENDATION_TYP,
LTAK.BDART as REQ_TYP_TEXT,
LTAK.BENUM as REQ_NBR,
LTAK.ISTWM AS TRNSFR_ORD_ACT_DUR_TM,
LTAK.LGBZO AS STG_AREA_NBR,
LTAK.LGNUM AS WHSE_NBR,
LTAK.LGTOR AS WHSE_DOOR_NBR,
LTAK.LZNUM AS TRNSPRT_REF_NBR,
LTAK.MBLNR AS PROD_MVMNT_TYP_NBR,
LTAK.NOITM AS TRNSFR_ORD_LN_CNT,
LTAK.PKNUM AS CTRL_CYCLE_NBR,
LTAK.PKPOS AS CIRCULATION_NBR,
LTAK.QUEUE AS WHSE_QUEUE_NBR,
LTAK.REFNR AS USER_DEFINED_GRP_TEXT,
LTAK.RSNUM AS DOC_RSRV_NBR,
LTAK.SOLEX AS PLND_EXTNL_PROC_TM,
LTAK.SOLWM AS PLND_WM_PROC_TM,
LTAK.SWABW AS ACT_PROC_PCT,
LTAK.TANUM AS TRNSFR_ORD_NBR,
LTAK.TAPRI AS TRNSFR_ORD_PRYRT_NBR,
LTAK.TBNUM AS TRNSFR_REQ_NBR,
LTAK.TBPRI AS TRNSFR_REQ_PRYRT_NBR,
LTAK.UBNUM AS POST_CHG_NBR,
LTAK.VBELN AS REF_DOC_NBR,
LTAK.KDISO AS ORD_DIFF_INFO,
LTAK.KR2KU AS ORD_ACCUM_INFO,
LTAK.KR2SO AS ORD_IMDT_INFO,
LTAK.PERNR AS WM_TRNSFR_ORD_PRSN_NBR

FROM {{source('SAPC11','LTAK')}} LTAK
WHERE LTAK.MANDT={{var('sapc11mandtftr')}}