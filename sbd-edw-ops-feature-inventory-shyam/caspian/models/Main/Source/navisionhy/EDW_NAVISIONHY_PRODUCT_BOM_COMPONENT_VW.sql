SELECT
UPPER(CONCAT(COALESCE(PRODUCTIONBOMLINE.COMPANY::VARCHAR,''),'~',COALESCE(PRODUCTIONBOMLINE.PRODUCTIONBOMNO::VARCHAR,''),'~',Coalesce(PRODUCTIONBOMLINE.VERSIONCODE::varchar,''),'~',Coalesce(TO_NUMBER(PRODUCTIONBOMLINE.LINENO)::varchar,''))) AS PROD_BOM_CMPNT_KEY,
'NAVISIONHY' AS {{var('column_srcsyskey')}},
MD5(UPPER(CONCAT(COALESCE(PRODUCTIONBOMLINE.COMPANY::VARCHAR,''),'~',COALESCE(PRODUCTIONBOMLINE.PRODUCTIONBOMNO::VARCHAR,''),'~',Coalesce(PRODUCTIONBOMLINE.VERSIONCODE::varchar,''),'~',Coalesce(TO_NUMBER(PRODUCTIONBOMLINE.LINENO)::varchar,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
PRODUCTIONBOMLINE.LOADDTS AS {{var('column_z3loddtm')}},
PRODUCTIONBOMLINE.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
PRODUCTIONBOMLINE.PRODUCTIONBOMNO AS PRODTN_BOM_NBR,
PRODUCTIONBOMLINE.VERSIONCODE AS VER_NBR,
PRODUCTIONBOMLINE.LINENO AS TXN_LN_NBR,
case when length(trim(PRODUCTIONBOMLINE.NO))<1 then {{var('default_mapkey')}} when PRODUCTIONBOMLINE.NO IS NULL then  {{var('default_mapkey')}} else (PRODUCTIONBOMLINE.NO) end AS PRODTN_BOM_LKEY,
PRODUCTIONBOMLINE.DESCRIPTION AS BOM_HDR_DESC,
PRODUCTIONBOMLINE.UNITOFMEASURECODE AS UOM_NBR,
PRODUCTIONBOMLINE.QUANTITY AS ASSEMBLY_ITEM_REQ_QTY,
PRODUCTIONBOMLINE.POSITION3 AS INVTY_PLACEMENT_3_PSTN,
PRODUCTIONBOMLINE.SCRAP AS SCRAP_PCT,
PRODUCTIONBOMLINE.STARTINGDATE AS PRODTN_START_DTE,
PRODUCTIONBOMLINE.ENDINGDATE AS PRODTN_END_DTE,
PRODUCTIONBOMLINE.QUANTITYPER AS PROD_PER_UNIT_QTY,
PRODUCTIONBOMLINE.TYPE AS PROD_CMPNT_TYP_LKEY,
PRODUCTIONBOMLINE.POSITION AS INVTY_PLACEMENT_1_PSTN,
PRODUCTIONBOMLINE.POSITION2 AS INVTY_PLACEMENT_2_PSTN,
PRODUCTIONBOMLINE.ROUTINGLINKCODE AS RTG_LINK_CD,
PRODUCTIONBOMLINE.CALCULATIONFORMULA AS QTY_CALC_FORMULA,
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
{{var('default_key')}} AS PROD_KEY,
{{var('default_key')}} AS CMPNT_PROD_KEY,
{{var('default_key')}} AS QTY_PER_TYP_LKEY,
{{var('default_key')}} AS PROD_GRP_TYP_LKEY,
{{var('default_key')}} AS CMDTY_CD_LKEY,
{{var('default_key')}} AS JOINT_PROD_TYP_LKEY,
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
{{var('default_key')}} AS BUS_UNIT_KEY,
{{var('default_key')}} AS PROD_LN_TYP_LKEY,
{{var('default_key')}} AS BASE_UOM_KEY,
{{var('default_key')}} AS BOM_KEY,
{{var('default_key')}} AS LAST_MTNC_USER_KEY,
{{var('default_key')}} AS PROD_CLS_LKEY,
PRODUCTIONBOMLINE.SOURCECOMPANY AS CO_NAME
FROM {{source('NAVISIONHY','PRODUCTIONBOMLINE')}} PRODUCTIONBOMLINE