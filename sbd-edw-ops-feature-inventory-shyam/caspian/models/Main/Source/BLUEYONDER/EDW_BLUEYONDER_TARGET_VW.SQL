SELECT
UPPER(CONCAT(COALESCE(trim(TARGET.TARGET),''))) AS TARGET_KEY,
'BLUEYONDER' as  {{var('column_srcsyskey')}},  
MD5(UPPER(CONCAT(COALESCE(trim(TARGET.TARGET),''    )))) AS {{var('column_rechashkey')}},
{{var('default_n')}} as {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' as {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ as {{var('column_ETL_INS_DTE')}},
'{{model.name}}' as {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ as {{var('column_ETL_UPD_DTE')}},
TARGET.LOADDTS as {{var('column_z3loddtm')}},
TARGET.LOADDTS as  {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' as {{var('column_verexpirydt')}},
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
UPPER(TARGET.TARGET)	AS	TARGET,
TARGET.ALLOCMETHOD	AS	ALLOCMETHOD,
TARGET.DMDCAL	AS	DMDCAL,
TARGET.STARTDATE	AS	FCST_START_DTE,
TARGET.ENDDATE	AS	ENDDATE,
TARGET.DESCR	AS	DESCR
FROM (select * from {{source('BLUEYONDER','TARGET')}} 
 QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(TARGET) ORDER BY LOADDTS ) =1 )TARGET
