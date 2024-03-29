SELECT
UPPER(CONCAT(COALESCE(PSMSTR.PS_PAR::VARCHAR,''),'~',COALESCE(PSMSTR.PS_COMP::VARCHAR,''),'~',COALESCE(PSMSTR.PS_START::VARCHAR,''),'~',COALESCE(PSMSTR.PS_REF::VARCHAR,''))) AS PROD_BOM_CMPNT_KEY,
'QADBR' AS {{var('column_srcsyskey')}},
MD5(UPPER(CONCAT(COALESCE(PSMSTR.PS_PAR::VARCHAR,''),'~',COALESCE(PSMSTR.PS_COMP::VARCHAR,''),'~',COALESCE(PSMSTR.PS_START::VARCHAR,''),'~',COALESCE(PSMSTR.PS_REF::VARCHAR,'')))) AS {{var('column_rechashkey')}},
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
case when length(trim(PSMSTR.PS_PAR))<1 then {{var('default_mapkey')}} when PSMSTR.PS_PAR IS NULL then  {{var('default_mapkey')}} else (PSMSTR.PS_PAR) end AS PROD_KEY,
case when length(trim(PSMSTR.PS_COMP))<1 then {{var('default_mapkey')}} when PSMSTR.PS_COMP IS NULL then  {{var('default_mapkey')}} else (PSMSTR.PS_COMP) end AS CMPNT_PROD_KEY,
PSMSTR.PS_QTY_PER AS PROD_QTY,
PSMSTR.PS_SCRP_PCT AS PROD_SCRAP_PCT,
TRY_TO_DATE(PSMSTR.PS_START,'MM/DD/YY') AS BOM_RELATION_START_DTE,
TRY_TO_DATE(PSMSTR.PS_END,'MM/DD/YY') AS BOM_RELATION_END_DTE,
PSMSTR.PS_OP AS OPER_STEP_NBR,
PSMSTR.PS_ITEM_NO AS PROD_STRC_RPT_NBR,
CASE WHEN UPPER(PSMSTR.PS_MANDATORY) = 'YES' THEN 'Y' ELSE 'N' END AS IS_CMPNT_ITEM_REQ_FLAG,
case when length(trim(PSMSTR.PS_QTY_TYPE))<1 then {{var('default_mapkey')}} when PSMSTR.PS_QTY_TYPE IS NULL then  {{var('default_mapkey')}} else (PSMSTR.PS_QTY_TYPE) end AS QTY_PER_TYP_LKEY,
case when length(trim(PSMSTR.PS_GROUP))<1 then {{var('default_mapkey')}} when PSMSTR.PS_GROUP IS NULL then  {{var('default_mapkey')}} else (PSMSTR.PS_GROUP) end AS PROD_GRP_TYP_LKEY,
CASE WHEN UPPER(PSMSTR.PS_CRITICAL) = 'YES' THEN 'Y' ELSE 'N' END AS CRTCL_CMPNT_ASSEMBLY_STAT_FLAG,
PSMSTR.PS_COMP_UM AS PROD_CMPNT_UNIT_VAL,
PSMSTR.PS_UM_CONV AS PROD_UM_CONV_FACTOR,
case when length(trim(PSMSTR.PS_COMM_CODE))<1 then {{var('default_mapkey')}} when PSMSTR.PS_COMM_CODE IS NULL then  {{var('default_mapkey')}} else (PSMSTR.PS_COMM_CODE) end AS CMDTY_CD_LKEY,
PSMSTR.PS_START_ECN AS PROD_BEG_ECN_NBR,
PSMSTR.PS_END_ECN AS PROD_END_ECN_NBR,
{{var('default_key')}} AS BOM_CTGY_LKEY,
{{var('default_key')}} AS RESIDING_LOC_KEY,
{{var('default_key')}} AS ISS_PLANT_LOC_KEY,
{{var('default_key')}} AS CMPNT_UOM_KEY,
{{var('default_key')}} AS BOM_CMPNT_ITEM_CTGY_LKEY,
{{var('default_key')}} AS CLS_TYP_LKEY,
{{var('default_key')}} AS DOC_TYP_LKEY,
{{var('default_key')}} AS PROD_GRP_LKEY,
{{var('default_key')}} AS PROD_BOM_PRNT_KEY,
--{{var('default_key')}} AS OPTIONAL_PROD_BOM_CMPNT_KEY,
-- {{var('default_key')}} AS AUXILIARY_UOM_KEY,
-- {{var('default_key')}} AS PRNT_PROD_KEY,
{{var('default_key')}} AS PROD_EXPLD_TYP_LKEY,
--{{var('default_key')}} AS PUR_ORG_KEY,
{{var('default_key')}} AS BOM_PROCUR_TYP_LKEY,
{{var('default_key')}} AS LANG_FMT_TYP_LKEY,
--{{var('default_key')}} AS LEADTIME_OFFSET_UOM_KEY,
{{var('default_key')}} AS PROD_OBJ_CTGY_LKEY,
{{var('default_key')}} AS PRODTN_SUPPLY_AREA_LKEY,
{{var('default_key')}} AS PROD_COST_RELVNT_LKEY,
{{var('default_key')}} AS CRNCY_KEY,
{{var('default_key')}} AS ALT_PROD_STRAT_LKEY,
--{{var('default_key')}} AS ALT_DISPLAY_FMT_KEY,
{{var('default_key')}} AS PROD_BOM_CTGY_LKEY,
--{{var('default_key')}} AS FORMULA_KEY,
{{var('default_key')}} AS PROD_BOM_REF_POINT_LKEY,
{{var('default_key')}} AS CMPNT_CNSMPTN_DIST_LKEY,
{{var('default_key')}} AS PRODTN_BOM_LKEY,
{{var('default_key')}} AS PROD_CMPNT_TYP_LKEY,
{{var('default_key')}} AS BUS_UNIT_KEY,
{{var('default_key')}} AS PROD_LN_TYP_LKEY,
{{var('default_key')}} AS BASE_UOM_KEY,
{{var('default_key')}} AS BOM_KEY,
{{var('default_key')}} AS LAST_MTNC_USER_KEY,
{{var('default_key')}} AS PROD_CLS_LKEY,
case when length(trim(PSMSTR.PS_JOINT_TYPE))<1 then {{var('default_mapkey')}} when PSMSTR.PS_JOINT_TYPE IS NULL then  {{var('default_mapkey')}} else (PSMSTR.PS_JOINT_TYPE) end AS JOINT_PROD_TYP_LKEY,
PSMSTR.PS_RMKS AS USER_RMRK_TEXT,
PSMSTR.PS_QTY_PER_B AS BATCH_SZ_QTY,
PSMSTR.PS_REF AS BOM_TEXT_REF
FROM {{source('QADBR','PSMSTR')}} PSMSTR
