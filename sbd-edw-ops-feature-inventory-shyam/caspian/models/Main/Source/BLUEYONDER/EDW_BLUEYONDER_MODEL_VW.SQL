SELECT
UPPER(CONCAT(MODEL.MODEL::VARCHAR,'')) AS MODEL_KEY,
'BLUEYONDER' AS  {{var('column_srcsyskey')}},  
MD5(UPPER(CONCAT(MODEL.MODEL::VARCHAR,''))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
MODEL.LOADDTS AS  {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
MODEL.LOADDTS AS {{var('column_z3loddtm')}},
MODEL.DESCR AS HIST_STREAM_DESC,
MODEL.HISTSTREAM AS HIST_STREAM,
MODEL.ALGORITHM AS MODL_PROC_ALGORITHM,
MODEL.MODEL AS PLNG_MODEL_NAME
FROM {{source('BLUEYONDER','MODEL')}} MODEL