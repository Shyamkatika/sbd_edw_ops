SELECT
UPPER(CONCAT(COALESCE(TARGETDFUMAP.DMDGROUP::VARCHAR,''),'~',COALESCE(TARGETDFUMAP.DMDUNIT::VARCHAR,''),'~',COALESCE(TARGETDFUMAP.LOC::VARCHAR,''),'~',COALESCE(TARGETDFUMAP.MODEL::VARCHAR,''),'~',COALESCE(TARGETDFUMAP.TARGET::VARCHAR,''))) AS TARGET_DFU_MAP_KEY,
'BLUEYONDER' AS  {{var('column_srcsyskey')}},  
MD5(UPPER(CONCAT(COALESCE(TARGETDFUMAP.DMDGROUP::VARCHAR,''),'~',COALESCE(TARGETDFUMAP.DMDUNIT::VARCHAR,''),'~',COALESCE(TARGETDFUMAP.LOC::VARCHAR,''),'~',COALESCE(TARGETDFUMAP.MODEL::VARCHAR,''),'~',COALESCE(TARGETDFUMAP.TARGET::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
TARGETDFUMAP.LOADDTS AS  {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
TARGETDFUMAP.LOADDTS AS {{var('column_z3loddtm')}},
TARGETDFUMAP.DMDGROUP AS DMD_GRP_KEY,
TARGETDFUMAP.DMDUNIT AS DMD_UNIT_KEY,
TARGETDFUMAP.LOC AS LOC_KEY,
TARGETDFUMAP.MODEL AS PLNG_MODEL_NAME,
TARGETDFUMAP.QTY AS TARGET_QTY,
UPPER(TARGETDFUMAP.TARGET) AS TARGET
FROM (select * from {{source('BLUEYONDER','TARGETDFUMAP')}}
 QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(DMDGROUP),UPPER(DMDUNIT),UPPER(LOC),upper(MODEL),UPPER(TARGET) ORDER BY LOADDTS ) =1 )TARGETDFUMAP
