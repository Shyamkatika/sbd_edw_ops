SELECT     
upper(DMDGROUP.DMDGROUP) AS DMD_GRP_KEY,
'BLUEYONDER' AS  {{var('column_srcsyskey')}},  
MD5(upper(DMDGROUP.DMDGROUP)) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
DMDGROUP.LOADDTS AS  {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
DMDGROUP.LOADDTS AS {{var('column_z3loddtm')}},
DMDGROUP.DESCR AS DMD_GRP_DESC,
DMDGROUP.U_DMD_GRP_LVL AS DMD_GRP_LVL,
DMDGROUP.U_DGLVL_1 AS DMD_GRP_LVL_1,
DMDGROUP.HIERARCHYLEVEL AS HIER_LVL,
DMDGROUP.U_BUS_SEG AS BUS_SEG
FROM {{source('BLUEYONDER','DMDGROUP')}} DMDGROUP
WHERE upper(DMDGROUP.DMDGROUP) <> ' '