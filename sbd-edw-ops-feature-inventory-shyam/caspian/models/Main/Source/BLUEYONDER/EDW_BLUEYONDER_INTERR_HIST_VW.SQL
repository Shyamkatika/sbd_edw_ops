SELECT
UPPER(CONCAT(COALESCE(trim(INTERR_HIST.ERROR_STAMP),''),'~',COALESCE(trim(INTERR_HIST.DMDUNIT),''),'~',COALESCE(trim(INTERR_HIST.DMDGROUP),''),'~',COALESCE(trim(INTERR_HIST.LOC),'')
,'~',COALESCE(trim(INTERR_HIST.HISTSTREAM),''),'~',COALESCE(trim(INTERR_HIST.EVENT),''),'~',COALESCE(trim(INTERR_HIST.STARTDATE),'')
,'~',COALESCE(trim(INTERR_HIST.TYPE),''),'~',COALESCE(trim(INTERR_HIST.INTEGRATION_STAMP),''),'~',COALESCE(trim(INTERR_HIST.INTEGRATION_JOBID),''))) AS INTERR_HIST_KEY,
'BLUEYONDER' as  {{var('column_srcsyskey')}},  
MD5(UPPER(CONCAT(COALESCE(trim(INTERR_HIST.ERROR_STAMP),''),'~',COALESCE(trim(INTERR_HIST.DMDUNIT),''),'~',COALESCE(trim(INTERR_HIST.DMDGROUP),''),'~',COALESCE(trim(INTERR_HIST.LOC),'')
,'~',COALESCE(trim(INTERR_HIST.HISTSTREAM),''),'~',COALESCE(trim(INTERR_HIST.EVENT),''),'~',COALESCE(trim(INTERR_HIST.STARTDATE),'')
,'~',COALESCE(trim(INTERR_HIST.TYPE),''),'~',COALESCE(trim(INTERR_HIST.INTEGRATION_STAMP),''),'~',COALESCE(trim(INTERR_HIST.INTEGRATION_JOBID),'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} as {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' as {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ as {{var('column_ETL_INS_DTE')}},
'{{model.name}}' as {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ as {{var('column_ETL_UPD_DTE')}},
INTERR_HIST.LOADDTS as {{var('column_z3loddtm')}},
INTERR_HIST.LOADDTS as  {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' as {{var('column_verexpirydt')}},
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
INTERR_HIST.ERROR_STAMP	AS	ERROR_STAMP,
INTERR_HIST.ERROR_STR	AS	ERROR_STR,
INTERR_HIST.DMDUNIT	AS	DMD_UNIT_KEY,
INTERR_HIST.DMDGROUP	AS	DMD_GRP_KEY,
INTERR_HIST.LOC	AS	LOC_KEY,
INTERR_HIST.HISTSTREAM	AS	HISTSTREAM,
INTERR_HIST.EVENT	AS	EVENT,
INTERR_HIST.STARTDATE	AS	FCST_START_DTE,
INTERR_HIST.TYPE	AS	TYPE,
INTERR_HIST.INTEGRATION_STAMP	AS	INTEGRATION_STAMP,
INTERR_HIST.INTEGRATION_JOBID	AS	INTEGRATION_JOBID,
INTERR_HIST.DUR	AS	FCST_DUR,
INTERR_HIST.QTY AS FCST_QTY
FROM {{source('BLUEYONDER','INTERR_HIST')}} INTERR_HIST