select
UPPER(CONCAT(coalesce(RKPF.RSNUM::VARCHAR,''))) AS INVTY_RSRV_HDR_KEY,
'SAPP10' AS {{var('column_srcsyskey')}},
MD5(UPPER(CONCAT(coalesce(RKPF.RSNUM::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
RKPF.LOADDTS AS {{var('column_z3loddtm')}},
RKPF.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},


RKPF.EBELP AS PUR_PROD_NBR,

case when length(trim(UPPER((RKPF.KDAUF)))) <1 then {{var('default_mapkey')}} when UPPER(RKPF.KDAUF)
IS NULL then {{var('default_mapkey')}} else UPPER(RKPF.KDAUF) end AS SLS_ORD_HDR_KEY,

RKPF.KDEIN AS DLV_SCHED_HDR_NBR,

RKPF.KDPOS AS PROD_NBR,

case when length(trim(UPPER((RKPF.KUNNR)))) <1 then {{var('default_mapkey')}} when UPPER(RKPF.KUNNR)
IS NULL then {{var('default_mapkey')}} else UPPER(RKPF.KUNNR) end AS CUST_KEY,

case when length(trim(UPPER((RKPF.KZVER)))) <1 then {{var('default_mapkey')}} when UPPER(RKPF.KZVER)
IS NULL then {{var('default_mapkey')}} else UPPER(RKPF.KZVER) end AS RSRV_LOC_LKEY,

RKPF.PARGB AS TRDG_PTNR_CO_ID,

RKPF.PRCTR AS PROFIT_CNTR_NBR,

RKPF.RSNUM AS PROD_RSRV_NBR,

--case when length(trim(UPPER((RKPF.UMLGO)))) <1 then {{var('default_mapkey')}} when UPPER(RKPF.UMLGO)
--IS NULL then {{var('default_mapkey')}} else UPPER(RKPF.UMLGO) end AS RCV_ISS_SLOC_KEY,

case when concat((RKPF.UMLGO),'~',(RKPF.UMWRK)) = ' ~ ' then {{var('default_mapkey')}}
    else concat((RKPF.UMLGO),'~',(RKPF.UMWRK)) end as RCV_ISS_SLOC_KEY,

case when length(trim(UPPER((RKPF.UMWRK)))) <1 then {{var('default_mapkey')}} when UPPER(RKPF.UMWRK)
IS NULL then {{var('default_mapkey')}} else UPPER(RKPF.UMWRK) end AS RCV_ISS_PLANT_KEY,

RKPF.WEMPF AS SHIPTO_CUST_ID,

RKPF.KOSTL AS CSTCTR_NAME,

RKPF.IMKEY AS REAL_ESTATE_OBJ_ITRL_NBR,

case when length(trim(UPPER((RKPF.PARBU)))) <1 then {{var('default_mapkey')}} when UPPER(RKPF.PARBU)
IS NULL then {{var('default_mapkey')}} else UPPER(RKPF.PARBU) end AS CLEARING_CO_CD_LKEY,

case  when UPPER(RKPF.XCALE) ='X'  then 'Y' else 'N'  end AS FACTORY_CLNDR_CHK_FLAG,


--RKPF.RSDAT AS RSRV_BASE_DTE,
--RKPF.DABRZ AS REF_SETLMNT_DTE,

TRY_TO_DATE(RKPF.RSDAT,'YYYYMMDD') AS RSRV_BASE_DTE,
TRY_TO_DATE(RKPF.DABRZ,'YYYYMMDD') AS REF_SETLMNT_DTE,

RKPF.RECID as ITRL_RCVRY_TYP,
RKPF.APLZL as ITRL_CNTR_NBR,
RKPF.BWART as MVMNT_TYP_NBR,
RKPF.EBELN as PO_NBR,
RKPF.AUFNR as ORD_NBR,
RKPF.PAOBJNR as PROFITABILITY_SEG_NBR,
RKPF.SERIE AS BOM_EXPLD_NBR,
RKPF.VPTNR AS PTNR_ACCT_NBR,
RKPF.PRZNR AS BUS_PROC,
RKPF.KOKRS AS COST_ACCTG_CD,
RKPF.LSTAR AS ACTVTY_TYP_CD,
RKPF.NPLNR AS ACCT_ASGN_NETWORK_NBR,
RKPF.ANLN1 AS INVTY_ASSET_NBR,
RKPF.ANLN2 AS INVTY_ASSET_SUB_NBR,
RKPF.AUFPL AS OPER_RTG_NBR,
RKPF.BUDGET_PD AS BUD_PERD,
RKPF.FIPOS AS COMMIT_ITEM_NBR,
RKPF.FISTL AS FUND_CNTR_ORGANIZATIONAL_NAME,
RKPF.FKBER AS FUNC_AREA,
RKPF.GEBER AS FUNDS_CNTR,
RKPF.GRANT_NBR AS FUND_GRANT_AMT,
RKPF.KSTRG AS COST_OBJ_NAME,
RKPF.PS_PSP_PNR AS WORK_BRKDN_STRC_TEXT

FROM {{source('SAPP10','RKPF')}} RKPF
WHERE RKPF.MANDT={{var('sapp10mandtftr')}}