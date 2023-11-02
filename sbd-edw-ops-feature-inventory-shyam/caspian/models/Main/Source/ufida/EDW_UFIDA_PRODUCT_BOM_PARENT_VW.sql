Select
TRUNC(BOMPARENT.BOMID) AS PROD_BOM_PRNT_KEY,
'UFIDA' AS {{var('column_srcsyskey')}},
MD5(TRUNC(BOMPARENT.BOMID)) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
BOMPARENT.LOADDTS AS {{var('column_z3loddtm')}},
BOMPARENT.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
TRUNC(BOMPARENT.ParentId) AS REL_PROD_BOM_PRNT_ID,
BOMPARENT.ParentScrap	as	PRNT_PROD_SCRAP_PCT
FROM {{source('UFIDA','BOMPARENT')}} BOMPARENT