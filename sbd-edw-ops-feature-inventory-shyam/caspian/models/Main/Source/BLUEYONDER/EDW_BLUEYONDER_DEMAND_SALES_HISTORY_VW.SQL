SELECT
UPPER(CONCAT(COALESCE(HIST.DMDUNIT::VARCHAR,''),'~',COALESCE(HIST.DMDGROUP::VARCHAR,''),'~',COALESCE(HIST.LOC::VARCHAR,''),'~',COALESCE(HIST.HISTSTREAM::VARCHAR,'')
,'~',COALESCE(HIST.STARTDATE::VARCHAR,''),'~',COALESCE(HIST.TYPE::VARCHAR,''),'~',COALESCE(HIST.EVENT::VARCHAR,''))) AS DMD_SLS_HIST_KEY ,
'BLUEYONDER' AS  {{var('column_srcsyskey')}},  
MD5(UPPER(CONCAT(COALESCE(HIST.DMDUNIT::VARCHAR,''),'~',COALESCE(HIST.DMDGROUP::VARCHAR,''),'~',COALESCE(HIST.LOC::VARCHAR,''),'~',COALESCE(HIST.HISTSTREAM::VARCHAR,'')
,'~',COALESCE(HIST.STARTDATE::VARCHAR,''),'~',COALESCE(HIST.TYPE::VARCHAR,''),'~',COALESCE(HIST.EVENT::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
HIST.LOADDTS AS  {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
HIST.LOADDTS AS {{var('column_z3loddtm')}},
HIST.LOC    AS  LOC_KEY,
HIST.DMDGROUP   AS  DMD_GRP_KEY,
HIST.DMDUNIT    AS  DMD_UNIT_KEY,
HIST.DUR    AS  DUR_PERD_WK_CNT,
HIST.EVENT  AS  OVRD_EVNT_NAME,
HIST.HISTSTREAM AS  HIST_STREAM_TEXT,
HIST.QTY    AS  HIST_FCST_QTY,
HIST.STARTDATE  AS  HIST_START_DTE,
HIST.TYPE   AS  DMD_HIST_TYP,
'EA' AS UOM_TYP
FROM {{source('BLUEYONDER','HIST')}} HIST