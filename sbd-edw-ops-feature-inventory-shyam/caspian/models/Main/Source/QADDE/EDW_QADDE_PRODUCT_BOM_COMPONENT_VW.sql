Select
concat(coalesce(cast(PSMSTR.ps_par as string),''),'~',coalesce(cast(PSMSTR.ps_comp as string),''),'~',coalesce(cast(PSMSTR.ps_ref as string),''),'~',coalesce(cast(PSMSTR.ps_start as string),'')) AS PROD_BOM_CMPNT_KEY,
'QADDE' AS {{var('column_srcsyskey')}},
MD5(concat(coalesce(cast(PSMSTR.ps_par as string),''),'~',coalesce(cast(PSMSTR.ps_comp as string),''),'~',coalesce(cast(PSMSTR.ps_ref as string),''),'~',coalesce(cast(PSMSTR.ps_start as string),''))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
PSMSTR.LOADDTS AS {{var('column_z3loddtm')}},
PSMSTR.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
PSMSTR.ps_par as PRNT_PROD_TYP,
case when length(trim(UPPER((PSMSTR.ps_comp)))) <1 then {{var('default_mapkey')}} when UPPER(PSMSTR.ps_comp)
IS NULL then {{var('default_mapkey')}} else UPPER(PSMSTR.ps_comp) end AS CMPNT_PROD_KEY,
--PSMSTR.ps_qty_per AS CMPNT_QTY, --dropped
PSMSTR.ps_scrp_pct AS PROD_SCRAP_PCT,
try_to_date(PSMSTR.ps_start) AS BOM_RELATION_START_DTE,-- ? is coming fromm source
try_to_date(PSMSTR.ps_end) AS BOM_RELATION_END_DTE,--? is coming fromm source
PSMSTR.ps_op AS OPER_STEP_NBR,
PSMSTR.ps_item_no AS PROD_STRC_RPT_NBR,
CASE WHEN PSMSTR.ps_mandatory ='yes' or 'YES' THEN 'Y' else 'N' end AS IS_CMPNT_ITEM_REQ_FLAG,
case when length(trim(UPPER((PSMSTR.ps_qty_type)))) <1 then {{var('default_mapkey')}} when UPPER(PSMSTR.ps_qty_type)
IS NULL then {{var('default_mapkey')}} else UPPER(PSMSTR.ps_qty_type) end AS QTY_PER_TYP_LKEY,
case when length(trim(UPPER((PSMSTR.ps_group)))) <1 then {{var('default_mapkey')}} when UPPER(PSMSTR.ps_group)
IS NULL then {{var('default_mapkey')}} else UPPER(PSMSTR.ps_group) end AS PROD_GRP_TYP_LKEY,
CASE WHEN PSMSTR.ps_critical = 'no' or 'NO' THEN 'N' else 'Y' end AS CRTCL_CMPNT_ASSEMBLY_STAT_FLAG,
PSMSTR.ps_qty_per_b AS BATCH_SZ_QTY,
PSMSTR.ps_comp_um AS PROD_CMPNT_UNIT_VAL,
PSMSTR.ps_um_conv AS PROD_UM_CONV_FACTOR,
case when length(trim(UPPER((PSMSTR.ps_comm_code)))) <1 then {{var('default_mapkey')}} when UPPER(PSMSTR.ps_comm_code)
IS NULL then {{var('default_mapkey')}} else UPPER(PSMSTR.ps_comm_code) end AS CMDTY_CD_LKEY,
PSMSTR.ps_start_ecn AS PROD_BEG_ECN_NBR,
PSMSTR.ps_end_ecn AS PROD_END_ECN_NBR,
case when length(trim(UPPER((PSMSTR.ps_joint_type)))) <1 then {{var('default_mapkey')}} when UPPER(PSMSTR.ps_joint_type)
IS NULL then {{var('default_mapkey')}} else UPPER(PSMSTR.ps_joint_type) end AS JOINT_PROD_TYP_LKEY
FROM {{source('QADDE','PSMSTR')}} PSMSTR 
