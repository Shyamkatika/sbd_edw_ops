with EDW_DEMAND_FORECAST_HISTORY as (
select 
UPPER(CONCAT(COALESCE(HISTFCST.DMDUNIT::VARCHAR,''),'~',  COALESCE(HISTFCST.DMDGROUP::VARCHAR,''), '~', 
             COALESCE(HISTFCST.LOC::VARCHAR,''),    '~',  COALESCE(HISTFCST.MODEL::VARCHAR,''),    '~', 
             COALESCE(HISTFCST.FCSTDATE::VARCHAR,''),'~', COALESCE(HISTFCST.STARTDATE::VARCHAR,'') )) AS DMD_FCST_HIST_KEY,
'BLUEYONDER' as  {{var('column_srcsyskey')}},  
MD5(UPPER(CONCAT(COALESCE(HISTFCST.DMDUNIT::VARCHAR,''),'~',COALESCE(HISTFCST.DMDGROUP::VARCHAR,''), '~', 
COALESCE(HISTFCST.LOC::VARCHAR,''),'~', COALESCE(HISTFCST.MODEL::VARCHAR,''),'~', COALESCE(HISTFCST.FCSTDATE::VARCHAR,'')
,'~', COALESCE(HISTFCST.STARTDATE::VARCHAR,'') ))) AS RCRD_HASH_KEY,
'N' as DEL_FROM_SRC_FLAG,
'EDW_BLUEYONDER_PRODUCT_VW' as ETL_INS_PID,
CURRENT_TIMESTAMP::TIMESTAMP_NTZ as ETL_INS_DTE,
'EDW_BLUEYONDER_PRODUCT_VW' as ETL_UPD_PID,
CURRENT_TIMESTAMP::TIMESTAMP_NTZ as ETL_UPD_DTE,
HISTFCST.LOADDTS as ZONE3_LOD_DTE,
HISTFCST.LOADDTS as  EFF_DTE,
'9999-12-31 00:00:00.000' as EXPR_DTE,
'Y' AS  CURR_RCRD_FLAG,                                                                                    
'N' AS  ORP_RCRD_FLAG,
 BASEFCST as BASE_FCST_QTY,
DMDGROUP as DMD_GRP_KEY,
DMDUNIT as DMD_UNIT_NAME,
FCSTDATE as CURR_FCST_DTE,
FCSTOVERRIDE as FCST_OVRD_QTY,
LAG as FCST_PERD_LAG_CNT,
LOC as LOC_KEY,
MARKETACTIVITY as MKT_ACTVTY_QTY,
NONBASEFCST as NON_BASE_FCST_QTY,
RECONCILEDFCST as RECON_FCST_QTY,
STARTDATE as FCST_START_DTE,
TOTFCSTLOCKADJ as ADJ_LOCK_FCST_QTY,
MODEL as MODEL_TYP,
  'DMDFCST' as FCST_STREAM_KEY,

DUR as FCST_DUR,
TARGETIMPACT as TGT_IMPT  
 from {{source('BLUEYONDER', 'HISTFCST')}} HISTFCST
  order by 
DMDUNIT,DMDGROUP,LOC,MODEL,FCSTDATE,STARTDATE
)
select distinct * from EDW_DEMAND_FORECAST_HISTORY
