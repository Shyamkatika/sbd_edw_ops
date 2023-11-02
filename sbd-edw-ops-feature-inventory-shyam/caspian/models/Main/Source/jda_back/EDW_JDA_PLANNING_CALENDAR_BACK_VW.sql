Select
CONCAT(coalesce(CAL.CAL::varchar,''),'~',coalesce(CALDATA.CAL::varchar,''),'~',coalesce(CALDATA.EFF::varchar,'')) AS PLNG_CLNDR_KEY,
'JDA' AS {{var('column_srcsyskey')}},
MD5(CONCAT(coalesce(CAL.CAL::varchar,''),'~',coalesce(CALDATA.CAL::varchar,''),'~',coalesce(CALDATA.EFF::varchar,''))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
CALDATA.LOADDTS AS {{var('column_z3loddtm')}},
CALDATA.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
--case when length(trim(UPPER((CAL.CAL)))) <1 then {{var('default_mapkey')}} when UPPER(CAL.CAL) IS NULL then {{var('default_mapkey')}} else UPPER(CAL.CAL) end AS CLNDR_NAME,
CAL.TYPE AS CLNDR_TYP,
CAL.NUMFCSTPER AS PERD_TYP,
CALDATA.OPT AS CLNDR_OPT,
CALDATA.AVAIL AS CLNDR_DYLY_CAPACITY_UOM_ID,
UPPER(CAL.CAL) as CLNDR_NAME,
CAL.DESCR as CLNDR_DESC,
CAL.MASTER AS MSTR_CLNDR_ID,
CAL.ROLLINGSW as CLNDR_ROLLING_PATTERN_TEXT,
CALDATA.ALTCAL as CLNDR_ALT_PATTERN_TEXT,
CAL.PATTERNSW as CLNDR_PATTERN_TEXT,
CAL.FIRSTDAYOFWEEK as WK_FIRST_DY_TEXT,
CALDATA.EFF as PLNG_CLNDR_EFF_SEC_CNT,
CALDATA.REPEAT as CLNDR_PATTERN_REPEAT_LEN_NBR,
CALDATA.PERWGT as CLNDR_PERD_WEIGHTAGE_VAL,
CALDATA.ALLOCWGT as ALLOC_PER_PERD_WEIGHTAGE_VAL,
CALDATA.COVDUR as ORD_CYCLE_FREQ_NAME,
CALDATA.DESCR as CLNDR_DESC_2
FROM {{source('JDA','CALDATA')}} CALDATA
LEFT JOIN  {{source('JDA','CAL')}} CAL ON (CALDATA.CAL) = (CAL.CAL)