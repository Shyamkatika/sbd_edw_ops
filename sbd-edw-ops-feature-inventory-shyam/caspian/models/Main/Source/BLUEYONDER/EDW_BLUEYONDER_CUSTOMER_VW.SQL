SELECT
UPPER(CUST.CUST) AS CUST_KEY,
'BLUEYONDER' as  {{var('column_srcsyskey')}},  
MD5(UPPER(CUST.CUST)) AS {{var('column_rechashkey')}},
{{var('default_n')}} as {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' as {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ as {{var('column_ETL_INS_DTE')}},
'{{model.name}}' as {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ as {{var('column_ETL_UPD_DTE')}},
CUST.LOADDTS as {{var('column_z3loddtm')}},
CUST.LOADDTS as  {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' as {{var('column_verexpirydt')}},
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
CUST.U_CUST_STATUS	AS CUST_STAT_LKEY,
CUST.DESCR	AS CUST_NAME,
CUST.U_CUST_DIV	AS	CUST_DIV_LKEY,
CUST.PRIORITY	AS	CUST_PRYRT,
case when CUST.U_CUSTORD IS NULL then 'U' when UPPER(CUST.U_CUSTORD) in ('YES','TRUE','1') then 'Y'
when UPPER(CUST.U_CUSTORD) in ('NO','FALSE','0') then 'N' else 'U' end as CUST_ORD_FLAG,
CUST.CUST	AS	CUST_NBR,
case when CUST.U_CUST_FCSTSW IS NULL then 'U' when UPPER(CUST.U_CUST_FCSTSW) in ('YES','TRUE','1') then 'Y'
when UPPER(CUST.U_CUST_FCSTSW) in ('NO','FALSE','0') then 'N' else 'U' end as FCST_ORD_FLAG,
CUST.U_DMDGROUP1	AS	DMD_GRP_1,
case when CUST.U_PRIM_CUST IS NULL then 'U' when UPPER(CUST.U_PRIM_CUST) in ('YES','TRUE','1') then 'Y'
when UPPER(CUST.U_PRIM_CUST) in ('NO','FALSE','0') then 'N' else 'U' end as PRIM_CUST_FLAG,
CUST.U_DMDGROUP2	AS	DMD_GRP_2,
CUST.U_DMDGROUP3	AS	DMD_GRP_3
FROM {{source('BLUEYONDER','CUST')}} CUST
WHERE UPPER(CUST.CUST) <> ' '