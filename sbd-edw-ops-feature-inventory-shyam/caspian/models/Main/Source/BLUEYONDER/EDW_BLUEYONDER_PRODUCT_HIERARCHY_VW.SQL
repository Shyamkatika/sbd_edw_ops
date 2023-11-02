with DMD as (
  select 
  UPPER(COALESCE(TRIM(DMDUNIT)::VARCHAR,'')) AS PROD_HIER_KEY,
'BLUEYONDER' as  {{var('column_srcsyskey')}},  
MD5(UPPER(COALESCE(TRIM(DMDUNIT)::VARCHAR,''))) AS {{var('column_rechashkey')}},
{{var('default_n')}} as {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' as {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ as {{var('column_ETL_INS_DTE')}},
'{{model.name}}' as {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ as {{var('column_ETL_UPD_DTE')}},
LOADDTS as {{var('column_z3loddtm')}},
LOADDTS as  {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' as {{var('column_verexpirydt')}},
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
UPPER(TRIM(DMDUNIT))  as PROD_KEY,
UPPER(TRIM(DMDUNIT)) as PROD_HIER_ID,
U_NEW_DMD_UNT_DTE as SRC_RCRD_CREATE_DTE,
U_DULVL_1 as PROD_HIER_LVL_1_KEY,
U_DULVL_1 as PROD_HIER_LVL_1_ID,
U_DULVL_1  as PROD_HIER_LVL_1_CD,
U_DULVL_2 as PROD_HIER_LVL_2_KEY,
U_DULVL_2 as PROD_HIER_LVL_2_ID,
U_DULVL_2  as PROD_HIER_LVL_2_CD,
'PLANNING ID' as PROD_HIER_LVL_1_DESC,
'FCSTGROUP' as PROD_HIER_LVL_2_DESC
FROM {{source('BLUEYONDER', 'DMDUNIT')}} DMD
)
select distinct * from DMD