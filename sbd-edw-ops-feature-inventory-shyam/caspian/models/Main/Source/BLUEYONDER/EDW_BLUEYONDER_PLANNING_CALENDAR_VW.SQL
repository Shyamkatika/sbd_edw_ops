with CAL as (
  select DISTINCT
UPPER(CAL) as PLNG_CLNDR_KEY,
'BLUEYONDER' as  {{var('column_srcsyskey')}}, 
MD5(UPPER(CAL)) AS {{var('column_rechashkey')}},
{{var('default_n')}} as {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' as {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ as {{var('column_ETL_INS_DTE')}},
'{{model.name}}' as {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ as {{var('column_ETL_UPD_DTE')}},
CAL.LOADDTS as {{var('column_z3loddtm')}},
CAL.LOADDTS as  {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' as {{var('column_verexpirydt')}},
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
CAL as CLNDR_NAME,
DESCR as CLNDR_DESC,
TYPE as CLNDR_TYP,
MASTER as MSTR_CLNDR_ID,
NUMFCSTPER as PERD_TYP,
ROLLINGSW as CLNDR_ROLLING_PATTERN_TEXT,
NULL as CLNDR_ALT_PATTERN_TEXT, 
PATTERNSW as CLNDR_PATTERN_TEXT,
NULL as WK_FIRST_DY_TEXT,
NULL as PLNG_CLNDR_EFF_SEC_CNT,
NULL as CLNDR_OPT,
NULL as CLNDR_PATTERN_REPEAT_LEN_NBR,
NULL as CLNDR_DYLY_CAPACITY_UOM_ID,
NULL as CLNDR_PERD_WEIGHTAGE_VAL,
NULL as ALLOC_PER_PERD_WEIGHTAGE_VAL,
NULL as CLNDR_DESC_2,
NULL as ORD_CYCLE_FREQ_NAME
FROM {{source('BLUEYONDER','CAL')}} CAL
), 

CALDATA as  (
      select DISTINCT
UPPER(CONCAT(COALESCE(CALDATA.CAL::VARCHAR,''),'~',COALESCE(CALDATA.EFF::VARCHAR,''))) as PLNG_CLNDR_KEY,
'BLUEYONDER' as  {{var('column_srcsyskey')}}, 
MD5(UPPER(CONCAT(COALESCE(CALDATA.CAL::VARCHAR,''),'~',COALESCE(CALDATA.EFF::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} as {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' as {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ as {{var('column_ETL_INS_DTE')}},
'{{model.name}}' as {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ as {{var('column_ETL_UPD_DTE')}},
CALDATA.LOADDTS as {{var('column_z3loddtm')}},
CALDATA.LOADDTS as  {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' as {{var('column_verexpirydt')}},
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
CAL as CLNDR_NAME,
NULL as CLNDR_DESC,
NULL as CLNDR_TYP,
NULL as MSTR_CLNDR_ID,
NULL as PERD_TYP,
NULL as CLNDR_ROLLING_PATTERN_TEXT,
ALTCAL as CLNDR_ALT_PATTERN_TEXT, 
NULL as  CLNDR_PATTERN_TEXT,
NULL as WK_FIRST_DY_TEXT,
EFF as PLNG_CLNDR_EFF_SEC_CNT,
OPT as CLNDR_OPT,
REPEAT as CLNDR_PATTERN_REPEAT_LEN_NBR,
AVAIL as CLNDR_DYLY_CAPACITY_UOM_ID,
PERWGT as CLNDR_PERD_WEIGHTAGE_VAL,
ALLOCWGT as ALLOC_PER_PERD_WEIGHTAGE_VAL,
DESCR as CLNDR_DESC_2,
COVDUR as ORD_CYCLE_FREQ_NAME
FROM {{source('BLUEYONDER','CALDATA')}} CALDATA
  ), 

 MAP as (
  select * from CAL
    union all 
  select * from CALDATA
  )
 select * from MAP