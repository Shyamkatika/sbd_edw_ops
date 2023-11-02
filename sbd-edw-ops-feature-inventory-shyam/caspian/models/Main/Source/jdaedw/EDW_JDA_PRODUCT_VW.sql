Select
COALESCE(ITEM.ITEM,'') AS PROD_KEY,
'JDAEDW' AS {{var('column_srcsyskey')}},
MD5(ITEM.ITEM) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
ITEM.LOADDTS AS {{var('column_z3loddtm')}},
ITEM.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
ITEM.DESCR AS SUPPLY_UNIT_DESC,--string too long
ITEM.DYNDEPQTY AS DYN_DPLY_QTY,
CASE WHEN (round(ITEM.PERISHABLESW)) = '1'  THEN 'Y' ELSE 'N' END AS PROD_PERISHABLE_FLAG,
ITEM.PRIORITY AS PROD_DMD_PRYRT_NBR,
ITEM.STORAGEGROUP AS PROD_STORG_GRP,--NA in SF
ITEM.U_BRAND_C11 AS SUPPLY_UNIT_BRAND_NAME_C11,
ITEM.U_BRAND_E03 AS SUPPLY_UNIT_BRAND_NAME_E03,
ITEM.U_CASEQTY AS CASE_QTY,
ITEM.U_CRSMATSTATUS_C11 AS CRS_MATL_STAT_C11,
ITEM.U_CRSMATSTATUS_E03 AS CRS_MATL_STAT_E03,
CASE WHEN (round(ITEM.U_DELETEFLAG)) = '1'  THEN 'Y' ELSE 'N' END AS DEL_FLAG,
ITEM.U_DIVISION_C11 AS SUPPLY_UNIT_DIV_C11,
ITEM.U_DUMMYITEM AS DUMMY_PROD,
ITEM.U_FUTPRODSTATUS AS FUTR_PROD_STAT,
ITEM.U_FUTSTATEFFDATE AS FUTR_PROD_EFF_DTE,
ITEM.U_GLOBAL_ABC AS GLB_ABC,
ITEM.U_GLOBALPLANNER AS GLB_PLNR,
ITEM.U_GPP_CATEGORY AS SUPPLY_GPP_CTGY_CD,
ITEM.U_GPP_DIVISION AS SUPPLY_GPP_DIV_NAME,
ITEM.U_GPP_PORTFOLIO_GRP AS SUPPLY_GPP_PORTFOLIO_GRP_NAME,
ITEM.U_GROUP_E03 AS SUPPLY_UNIT_GROUP_E03,
ITEM.U_IO_MATERIALTYPE AS IO_MATL_TYP,
ITEM.U_LIFECYCLE AS LIFE_CYCLE,
ITEM.U_MAIN_GRP_C11 AS SUPPLY_UNIT_MAIN_GRP_C11_NAME,
ITEM.U_MAKEBUY AS MAKE_BUY,
ITEM.U_MASTERCOIL AS MSTR_COIL,--NA in SF
ITEM.U_MASTERPACK AS MSTR_PKG,
ITEM.U_MATERIALTYPE_C11 AS MATL_TYP_C11,
ITEM.U_MATERIALTYPE_E03 AS MATL_TYP_E03,
ITEM.U_PLANNUM AS PLAN_NBR,
ITEM.U_PLANTCODE AS PLANT_CD,
ITEM.U_PRI_OVERRIDE AS PRC_OVRD,
ITEM.U_PROD_LINE_C11 AS SUPPLY_PROD_LN_C11,--NA in SF
ITEM.U_PROD_LINE_E03 AS SUPPLY_PROD_LN_E03,--NA in SF
ITEM.U_SAPALLOCATION_C11 AS SAP_ALLOC_C11,
ITEM.U_THICKNESS_INCH AS SUPPLY_PROD_THICKNESS_IN_INCH,--NA in SF
ITEM.U_THICKNESS_MM AS SUPPLY_PROD_THICKNESS_IN_MM,--NA in SF
ITEM.U_TPI AS SUPPLY_TEETH_PER_INCH,--NA in SF
ITEM.U_ULTIMSOURCE AS ULTM_SRC,
ITEM.U_ULTIMSOURCE_LT AS ULTM_SRC_LT,
ITEM.U_VENDOR AS VEND,
ITEM.U_WIDTH_INCH AS SUPPLY_PROD_WDTH_IN_INCH,--NA in SF
ITEM.U_WIDTH_MM AS SUPPLY_PROD_WDTH_IN_MM,--NA in SF
ITEM.UNITPRICE AS PROD_UNIT_PRC,
ITEM.UOM AS SUPPLY_UOM_DESC,
ITEM.VOL AS VOL,
ITEM.WGT AS WGT,
ITEM.U_MAIN_GRP_E03 AS SUPPLY_UNIT_MAIN_GRP_E03_NAME,
ITEM.ALLOCPOLICY AS PROD_ALLOC_POLICY,
ITEM.CALCCUMLEADTIMESW AS CUM_LEADTIME_DUR_CALC,
ITEM.DDSKUSW AS DYN_DPLY_SKU_VAL,
ITEM.DDSRCCOSTSW AS DYN_DPLY_SRC_COST,
ITEM.DEFAULTUOM::VARCHAR AS UOM_ID,
ITEM.DYNDEPDECIMALS AS DYN_DPLY_DECIMAL_PLACE,
ITEM.DYNDEPOPTION AS DYN_DPLY_OPT,
ITEM.DYNDEPPUSHOPT AS DYN_DPLY_PUSH_PULL_INDCTR,
ITEM.ITEMCLASS AS ITEM_CLS,
ITEM.PLANLEVEL AS ITEM_PLAN_LVL,
ITEM.RESTRICTPLANMODE AS RESTRCT_PLAN_MODE,
ITEM.SUPSNGROUPNUM AS SUPERSESSION_GRP_NBR,
ITEM.UNITSPERPALLET AS PROD_PAL_UNIT_NBR,
ITEM.U_DIVISION_E03 AS SUPPLY_UNIT_DIV_E03, 
ITEM.U_GROUP_C11  AS SUPPLY_UNIT_GROUP_C11, 

DMDUNIT.DMDUNIT AS DMD_UNIT_NAME,
DMDUNIT.DESCR AS DMD_UNIT_DESC,--string too long
DMDUNIT.U_DIVISION_C11 AS DMD_UNIT_DIV_C11,
DMDUNIT.U_MAIN_GRP_C11 AS DMD_UNIT_MAIN_GRP_NAME,
DMDUNIT.U_BRAND_E03 AS DMD_UNIT_BRAND_NAME_E03,
DMDUNIT.U_BRAND_C11 AS DMD_UNIT_BRAND_NAME_C11,
TO_TIMESTAMP(DMDUNIT.U_DU_BORN) AS DMD_UNIT_CREATE_DTE,
DMDUNIT.U_GPP_CATEGORY AS DMD_GPP_CTGY_CD,
DMDUNIT.U_GPP_DIVISION AS DMD_GPP_DIV_NAME,
DMDUNIT.U_GPP_PORTFOLIO_GRP AS DMD_GPP_PORTFOLIO_GRP_NAME,
DMDUNIT.U_GPP_SBU AS GPP_SBU_NAME,
TO_TIMESTAMP(DMDUNIT.U_PRODSTAT_EFFDATE_C11) AS STAT_EFF_C11_DTE,
TO_TIMESTAMP(DMDUNIT.U_PRODSTAT_EFFDATE_E03) AS STAT_EFF_E03_DTE,--NA in SF
TO_TIMESTAMP(DMDUNIT.U_USA_PRODLAUNCHDATE) AS PROD_LAUNCH_DTE,
DMDUNIT.U_GPP_BASIC_SBU AS GPP_BASIC_SBU_NAME,
DMDUNIT.U_GPP_BASIC_DIVISION AS GPP_BASIC_DIV_NAME,
DMDUNIT.U_GLB_SOURCE_OF_SUPPLY AS GLB_SUPPLY_SRC_NAME,
DMDUNIT.U_GLB_PREFERRED_VENDOR AS GLB_PREF_VEND_NAME,
DMDUNIT.U_GLB_SOS_MFGPURCHIND AS GLB_SOS_PUR,
DMDUNIT.U_GLB_SOURCE_OF_SUPPLY_AGG AS GLB_SOS_AGGREGATED,
DMDUNIT.U_DU_LEAD_TIME AS PROD_LEADTIME_MTH_CNT,
DMDUNIT.U_PLANT AS MFG_LOC,
DMDUNIT.U_ORIGINCNTRY AS CNTRY_CD,
DMDUNIT.U_RNR_RPTR_STGR AS USER_TEXT,
DMDUNIT.U_DU_ABC AS VELOCITY_CD,
DMDUNIT.U_BRAND_ROLLUP_C11 AS BRAND_ROLL_UP_C11_TEXT,--NA in SF
DMDUNIT.U_BRAND_ROLLUP_E03 AS BRAND_ROLL_UP_E03_TEXT,--NA in SF
DMDUNIT.U_PROD_LINE_C11 AS DMD_PROD_LN_C11,--NA in SF
DMDUNIT.U_PROD_LINE_E03 AS DMD_PROD_LN_E03,--NA in SF
TRY_TO_NUMBER(DMDUNIT.U_TPI) AS DMD_TEETH_PER_INCH,--NA in SF
TRY_TO_NUMBER(DMDUNIT.U_WIDTH_MM) AS DMD_PROD_WDTH_IN_MM,--NA in SF
TRY_TO_NUMBER(DMDUNIT.U_WIDTH_INCH) AS DMD_PROD_WDTH_IN_INCH,--NA in SF
TRY_TO_NUMBER(DMDUNIT.U_THICKNESS_MM) AS DMD_PROD_THICKNESS_IN_MM,--NA in SF
TRY_TO_NUMBER(DMDUNIT.U_THICKNESS_INCH) AS DMD_PROD_THICKNESS_IN_INCH,--NA in SF
DMDUNIT.UOM AS DMD_UOM_DESC,--NA in SF
DMDUNIT.U_DIVISION_E03 AS DMD_UNIT_DIV_E03
FROM {{source('JDAEDW','ITEM')}} ITEM 
LEFT JOIN {{source('JDAEDW','DMDUNIT')}} DMDUNIT ON ITEM.ITEM = DMDUNIT.DMDUNIT 
