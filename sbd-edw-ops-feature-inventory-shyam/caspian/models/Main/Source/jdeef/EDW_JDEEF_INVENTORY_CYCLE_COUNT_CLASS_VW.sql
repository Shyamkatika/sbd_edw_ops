select 
UPPER(COALESCE(F4140.PICYNO::varchar,'')) as CYCLE_CNT_CLS_KEY,
'JDEEF' AS  {{var('column_srcsyskey')}},
MD5(UPPER(COALESCE(F4140.PICYNO::varchar,''))) AS  {{var('column_rechashkey')}},
{{var('default_n')}} as {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS  {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS  {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_UPD_DTE')}},
F4140.LOADDTS AS  {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS  {{var('column_verexpirydt')}},
{{var('default_y')}} as {{var('column_currrecflag')}},
{{var('default_n')}} as {{var('column_orprecflag')}},
F4140.LOADDTS AS  {{var('column_z3loddtm')}},
case when length(trim(UPPER((F4140.PIUSER)))) <1 then {{var('default_mapkey')}} when UPPER(F4140.PIUSER)
IS NULL then {{var('default_mapkey')}} else UPPER(F4140.PIUSER) end AS CYCLE_CNT_CREATE_USER_KEY,
F4140.PICYIT AS CYCLE_CNT_ITEM_NBR,
F4140.PICYLO AS CYCLE_CNT_LOC_CNT,
F4140.PICYNO AS CYCLE_CNT_NBR,
F4140.PICSDJ AS CYCLE_CNT_START_DTE,
case when length(trim(UPPER((F4140.PICYCS)))) <1 then {{var('default_mapkey')}} when UPPER(F4140.PICYCS)
IS NULL then {{var('default_mapkey')}} else UPPER(F4140.PICYCS) end AS CYCLE_CNT_STAT_LKEY,
case when length(trim(UPPER((F4140.PIDSC1)))) <1 then {{var('default_mapkey')}} when UPPER(F4140.PIDSC1)
IS NULL then {{var('default_mapkey')}} else UPPER(F4140.PIDSC1) end AS CYCLE_CNT_TYP_KEY,
F4140.PIJOBN AS WORK_STATION_NBR
from {{source('JDEEF','F4140')}} F4140 