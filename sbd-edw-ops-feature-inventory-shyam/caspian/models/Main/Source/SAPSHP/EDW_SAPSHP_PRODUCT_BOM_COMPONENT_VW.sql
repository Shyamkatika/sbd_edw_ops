SELECT
UPPER(CONCAT(COALESCE(STPO.STLTY::VARCHAR,''),'~',COALESCE(STPO.STLNR::VARCHAR,''),'~',COALESCE(STPO.STLKN::VARCHAR,''),'~',COALESCE(STPO.STPOZ::VARCHAR,''))) AS PROD_BOM_CMPNT_KEY,
'SAPSHP' AS {{var('column_srcsyskey')}},
MD5(UPPER(CONCAT(COALESCE(STPO.STLTY::VARCHAR,''),'~',COALESCE(STPO.STLNR::VARCHAR,''),'~',COALESCE(STPO.STLKN::VARCHAR,''),'~',COALESCE(STPO.STPOZ::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
STPO.LOADDTS AS {{var('column_z3loddtm')}},
STPO.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
STPO.STLNR AS BOM_NBR,
STPO.VGKNT AS PRIOR_NODE_NBR,
STPO.STLKN AS BOM_CMPNT_SEQ_NBR,
STPO.POSNR AS BOM_LN_ITEM_NBR,
case when length(trim(STPO.STLTY))<1 then {{var('default_mapkey')}} when STPO.STLTY IS NULL then  {{var('default_mapkey')}} else (STPO.STLTY) end AS BOM_CTGY_LKEY,
STPO.MENGE AS PROD_QTY,
CASE WHEN STPO.FMENG = 'X' THEN 'Y' ELSE 'N' END AS FIXED_QTY_FLAG,
STPO.AVOAU AS OPER_SCRAP_QTY,
STPO.AUSCH AS PROD_SCRAP_PCT,
case when length(trim(STPO.MEINS))<1 then {{var('default_mapkey')}} when STPO.MEINS IS NULL then  {{var('default_mapkey')}} else (STPO.MEINS) end AS CMPNT_UOM_KEY,
case when length(trim(STPO.POSTP))<1 then {{var('default_mapkey')}} when STPO.POSTP IS NULL then  {{var('default_mapkey')}} else (STPO.POSTP) end AS BOM_CMPNT_ITEM_CTGY_LKEY,
case when length(trim(STPO.KLART))<1 then {{var('default_mapkey')}} when STPO.KLART IS NULL then  {{var('default_mapkey')}} else (STPO.KLART) end AS CLS_TYP_LKEY,
STPO.CLSZU AS BOM_CLS_NBR,
CASE WHEN STPO.CLOBK = 'X' THEN 'Y' ELSE 'N' END AS REQ_CMPNT_FLAG,
STPO.CUFACTOR AS COMPATIBLE_UNIT_INSTANCES_NBR,
STPO.AWAKZ AS BOM_CONFIGURABLE_TYP,
STPO.CVIEW AS ORG_FUNC_AREA_NAME,
STPO.POTX1 AS BOM_PROD_TEXT,
case when length(trim(STPO.MATKL))<1 then {{var('default_mapkey')}} when STPO.MATKL IS NULL then  {{var('default_mapkey')}} else (STPO.MATKL) end AS PROD_GRP_LKEY,
STPO.IDVAR AS PROD_CMPNT_VARIANT_TEXT,
STPO.INTRM AS INTRA_MATL_TEXT,
STPO.KNOBJ AS ASGN_DEPENDENCY_OBJ_NBR,
CASE WHEN STPO.SANFE = 'X' THEN 'Y' ELSE 'N' END AS PRODTN_RELVNT_FLAG,
STPO.STPOZ AS ITRL_CNTR_NBR,
STPO.AEHLP AS DTE_SHFT_HIER_TYP,
CASE WHEN STPO.ALEKZ = 'X' THEN 'Y' ELSE 'N' END AS ALE_STAT_FLAG,
STPO.ALPRF AS ALT_PROD_PSTN_NBR,
STPO.BEIKZ AS MATL_PRVN_TYP,
STPO.CSSTR AS AVG_MATL_PURITY_PCT,
STPO.DOKVR AS DOC_VER_NBR,
case when length(trim(STPO.DSPST))<1 then {{var('default_mapkey')}} when STPO.DSPST IS NULL then  {{var('default_mapkey')}} else (STPO.DSPST) end AS PROD_EXPLD_TYP_LKEY,
STPO.EKORG AS PUR_ORG_ID,
STPO.EWAHR AS USAGE_PRBLTY_PCT,
STPO.GUIDX AS PROD_CHG_STAT_ID,
STPO.IDHIS AS DOC_HIST_CNTR_NBR,
STPO.ITMID AS EXTNL_PROD_TYP,
case when length(trim(STPO.ITSOB))<1 then {{var('default_mapkey')}} when STPO.ITSOB IS NULL then  {{var('default_mapkey')}} else (STPO.ITSOB) end AS BOM_PROCUR_TYP_LKEY,
STPO.KSTKN AS ORIG_SALE_PROD_NODE_NBR,
STPO.KSTPZ AS ORIG_SALE_PROD_CNTR_NBR,
CASE WHEN STPO.KZNFP = 'X' THEN 'Y' ELSE 'N' END AS PROD_FOLLOW_UP_FLAG,
CASE WHEN STPO.LKENZ = 'X' THEN 'Y' ELSE 'N' END AS BOM_PROD_DEL_FLAG,
case when length(trim(STPO.LTXSP))<1 then {{var('default_mapkey')}} when STPO.LTXSP IS NULL then  {{var('default_mapkey')}} else (STPO.LTXSP) end AS LANG_FMT_TYP_LKEY,
CASE WHEN STPO.NETAU = 'X' THEN 'Y' ELSE 'N' END AS NEW_SCRAP_INDCTR_FLAG,
STPO.NFEAG AS DISCONTINUATION_GRP_NAME,
STPO.NFGRP AS FOLLOW_UP_GRP_NAME,
STPO.NLFMV AS LEADTIME_OFFSET_UOM,
STPO.NLFZV AS OPER_LEADTIME_DY_CNT,
STPO.OBJTY AS BOM_PROD_OBJ_GRP_NAME,
STPO.PEINH AS PROD_UNIT_PRC,
STPO.PREIS AS NON_STK_PROD_COST,
case when length(trim(STPO.PRVBE))<1 then {{var('default_mapkey')}} when STPO.PRVBE IS NULL then  {{var('default_mapkey')}} else (STPO.PRVBE) end AS PRODTN_SUPPLY_AREA_LKEY,
case when STPO.REKRI = 'X' THEN 'Y' ELSE 'N' END AS RECURSIVE_BOM_FLAG,
case when STPO.REKRS = 'X' THEN 'Y' ELSE 'N' END AS RECURSIVENESS_CHK_FLAG,
STPO.ROANZ AS VARBL_SZ_PROD_TYP_CNT,
STPO.ROMEN AS VARBL_SZ_PROD_TOT_QTY,
STPO.RVREL AS RELVNT_PROD_SLS_TYP,
STPO.SAKTO AS GL_ACCT_NBR,
case when STPO.SANIN = 'X' THEN 'Y' ELSE 'N' END AS PROD_PLANT_MTNC_FLAG,
case when length(trim(STPO.SANKA))<1 then {{var('default_mapkey')}} when STPO.SANKA IS NULL then  {{var('default_mapkey')}} else (STPO.SANKA) end AS PROD_COST_RELVNT_LKEY,
case when STPO.SANKO = 'X' THEN 'Y' ELSE 'N' END AS ENGNR_RELVNT_PROD_FLAG,
case when STPO.SCHGT = 'X' THEN 'Y' ELSE 'N' END AS BULK_MATL_FLAG,
case when STPO.UPSKZ = 'X' THEN 'Y' ELSE 'N' END AS SUB_PROD_BOM_EXIST_FLAG,
case when STPO.VALKZ = 'X' THEN 'Y' ELSE 'N' END AS CMPNT_PRESENT_IN_MULT_ALT_FLAG,
case when length(trim(STPO.WAERS))<1 then {{var('default_mapkey')}} when STPO.WAERS IS NULL then  {{var('default_mapkey')}} else (STPO.WAERS) end AS CRNCY_KEY,
STPO.WEBAZ AS RCPT_PROC_DY_CNT,
STPO.ALPGR AS ALT_PROD_GRP_NBR,
case when STPO.ALPOS = 'X' THEN 'Y' ELSE 'N' END AS ALT_PROD_FLAG,
case when length(trim(STPO.ALPST))<1 then {{var('default_mapkey')}} when STPO.ALPST IS NULL then  {{var('default_mapkey')}} else (STPO.ALPST) end AS ALT_PROD_STRAT_LKEY,
case when STPO.CADPO = 'X' THEN 'Y' ELSE 'N' END AS CAD_FLAG,
STPO.CLALT AS ALT_DISPLAY_FMT,
case when STPO.CLMUL = 'X' THEN 'Y' ELSE 'N' END AS MULT_SELECTION_ALLOW_FLAG,
STPO.ERSKZ AS SPARE_PART_TYP,
STPO.FUNCID AS FUNC_ID_TYP,
case when STPO.INSKZ = 'X' THEN 'Y' ELSE 'N' END AS INSTANCE_FLAG,
STPO.KNDBZ AS PROD_BOM_CHG_TYP,
STPO.KNDVB AS BOM_SLS_ORD_CHG_TYP,
case when length(trim(STPO.KSTTY))<1 then {{var('default_mapkey')}} when STPO.KSTTY IS NULL then  {{var('default_mapkey')}} else (STPO.KSTTY) end AS PROD_BOM_CTGY_LKEY,
STPO.KZCLB AS PROD_BOM_SELECTION_TYP,
case when STPO.KZKUP = 'X' THEN 'Y' ELSE 'N' END AS SUB_PROD_FLAG,
STPO.LIFNR  AS VEND_ACCT_NBR,
STPO.RFORM AS FORMULA,
case when length(trim(STPO.RFPNT))<1 then {{var('default_mapkey')}} when STPO.RFPNT IS NULL then  {{var('default_mapkey')}} else (STPO.RFPNT) end AS PROD_BOM_REF_POINT_LKEY,
case when STPO.SANVS = 'X' THEN 'Y' ELSE 'N' END AS PROD_ORIG_CONFIGURATION_FLAG,
case when STPO.STKKZ  = 'X' THEN 'Y' ELSE 'N' END AS PLANT_MTNC_ASSEMBLY_FLAG,
STPO.TECHV AS TECH_STAT_TEXT,
case when STPO.TPEKZ = 'X' THEN 'Y' ELSE 'N' END AS PROD_BOM_RESTRICTIONS_FLAG,
case when STPO.VACKZ = 'X' THEN 'Y' ELSE 'N' END AS AUTO_CONFIGURATION_DISPLAY_FLAG,
case when STPO.VCEKZ = 'X' THEN 'Y' ELSE 'N' END AS CONFIGURATION_EDITOR_DISPLAY_FLAG,
case when length(trim(STPO.VERTI))<1 then {{var('default_mapkey')}} when STPO.VERTI IS NULL then  {{var('default_mapkey')}} else (STPO.VERTI) end AS CMPNT_CNSMPTN_DIST_LKEY,
case when STPO.VSTKZ = 'X' THEN 'Y' ELSE 'N' END AS SINGLE_LVL_CONFIGURATION_DISPLAY_FLAG,
STPO.STVKN AS PROD_NODE_NBR,
TRY_TO_DATE(STPO.DATUV,'YYYY.MM.DD') AS BOM_VALID_TO_DTE,
case when length(trim(STPO.IDPOS))<1 then {{var('default_mapkey')}} when STPO.IDPOS IS NULL then  {{var('default_mapkey')}} else (STPO.IDPOS) end AS PROD_GRP_TYP_LKEY,
case when length(trim(STPO.IDNRK))<1 then {{var('default_mapkey')}} when STPO.IDNRK IS NULL then  {{var('default_mapkey')}} else (STPO.IDNRK) end AS CMPNT_PROD_KEY,
STPO.KSTNR AS SLS_ORD_BOM_ID,
case when length(trim(STPO.LGORT))<1 then {{var('default_mapkey')}} when STPO.LGORT IS NULL then  {{var('default_mapkey')}} else (STPO.LGORT) end AS RESIDING_LOC_KEY,
case when length(trim(STPO.PSWRK))<1 then {{var('default_mapkey')}} when STPO.PSWRK IS NULL then  {{var('default_mapkey')}} else (STPO.PSWRK) end AS ISS_PLANT_LOC_KEY,
STPO.AENNR AS BOM_CHG_NBR,
STPO.CLASS AS CLS_NBR,
STPO.EKGRP AS PUR_GRP_NAME,
case when length(trim(STPO.DOKAR))<1 then {{var('default_mapkey')}} when STPO.DOKAR IS NULL then  {{var('default_mapkey')}} else (STPO.DOKAR) end AS DOC_TYP_LKEY,
STPO.DOKNR AS DOC_NBR,
STPO.DOKTL AS DOC_PART_NAME,
TRY_TO_DATE(STPO.DVDAT,'YYYY.MM.DD') AS LAST_SHFT_DTE,
STPO.DVNAM AS VALID_FROM_DTE_CHG_USER_NAME,
STPO.POTX2 AS PROD_ADDTN_DESC,
STPO.NLFZT AS LEADTIME_OFFSET_DYS,
STPO.LIFZT AS DLV_TM_IN_DYS,
STPO.ROMEI AS UOM_SZ,
STPO.ROMS1 AS ONE_DIM_VARBL_SZ_1,
STPO.ROMS2 AS ONE_DIM_VARBL_SZ_2,
STPO.ROMS3 AS ONE_DIM_VARBL_SZ_3,
STPO.SORTF AS BOM_SORTING_STRING,
STPO.VGPZL AS PREV_PROD_CNTR,
case when length(trim(STPO.POTPR))<1 then {{var('default_mapkey')}} when STPO.POTPR IS NULL then  {{var('default_mapkey')}} else (STPO.POTPR) end AS PROD_OBJ_CTGY_LKEY,
STPO.SGT_CMKZ AS BOM_CMPNT_SEGMENTATION,
TRY_TO_NUMBER(STPO.SGT_CATV) AS SEGMENTATION_VAL,
TRY_TO_DATE(coalesce(STPO.VALID_TO,STPO.VALID_TO_RKEY),'YYYY.MM.DD') AS BOM_VALID_END_DTE,
coalesce(STPO.ECN_TO,STPO.ECN_TO_RKEY) AS CHG_MSTR_NBR,
STPO.ABLAD AS UNLOD_POINT,
STPO.WEMPF AS PROD_RCPNT_NAME,
STPO.STVKN_VERSN AS BOM_INHERIT_NODE_NBR,
STPO.LASTCHANGEDATETIME AS UTC_TM_STAMP,
STPO.SFWIND AS SOFTWARE_CMPNT_TYP,
STPO.DUMMY_STPO_INCL_EEW_PS AS PROD_BOM_DATA_ELMNT,
STPO.SAPMP_MET_LRCH AS LEN_CALC_MTHD,
STPO.SAPMP_MAX_FERTL AS MAX_PRODTN_LEN,
STPO.SAPMP_FIX_AS_J AS FIXED_SCRAP_ANY_LEN,
STPO.SAPMP_FIX_AS_E AS FIXED_SCRAP_FIRST_LEN,
STPO.SAPMP_FIX_AS_L AS FIXED_SCRAP_LAST_LEN,
STPO.SAPMP_ABL_ZAHL AS RUN_IN_LEN_NBR,
STPO.SAPMP_RUND_FAKT AS CMPNT_RND_VAL,
{{var('default_key')}} AS PROD_BOM_PRNT_KEY,
--{{var('default_key')}} AS OPTIONAL_PROD_BOM_CMPNT_KEY,
-- {{var('default_key')}} AS AUXILIARY_UOM_KEY,
-- {{var('default_key')}} AS PRNT_PROD_KEY,
{{var('default_key')}} AS PROD_KEY,
{{var('default_key')}} AS QTY_PER_TYP_LKEY,
{{var('default_key')}} AS CMDTY_CD_LKEY,
{{var('default_key')}} AS JOINT_PROD_TYP_LKEY,
{{var('default_key')}} AS PRODTN_BOM_LKEY,
{{var('default_key')}} AS PROD_CMPNT_TYP_LKEY,
{{var('default_key')}} AS BUS_UNIT_KEY,
{{var('default_key')}} AS PROD_LN_TYP_LKEY,
{{var('default_key')}} AS BASE_UOM_KEY,
{{var('default_key')}} AS BOM_KEY,
{{var('default_key')}} AS LAST_MTNC_USER_KEY,
{{var('default_key')}} AS PROD_CLS_LKEY,
TRY_TO_NUMBER(STPO.FSH_VMKZ) AS CMPNT_DEVN_VAL,
TRY_TO_NUMBER(STPO.FSH_PGQR) AS DIST_PRFL_QTY,
TRY_TO_NUMBER(STPO.FSH_PGQRRF) AS DIST_PRFL_REF_QTY,
STPO.FSH_CRITICAL_COMP AS FEASIBILITY_CRTCL_TYP,
STPO.FSH_CRITICAL_LEVEL AS BOM_CMPNT_CRTCL_LVL
FROM {{source('SAPSHP','STPO')}} STPO
WHERE STPO.MANDT ={{var('sapshpmandtftr')}}