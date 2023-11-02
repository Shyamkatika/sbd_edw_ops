{% macro ReleaseObjects() %}

USE SCHEMA CONSOLIDATED;

create or replace view VW_EDW_PRODUCT_BOM_COMPONENT(
	PROD_BOM_CMPNT_KEY,
	SRC_SYS_KEY,
	SRC_RCRD_CREATE_DTE,
	SRC_RCRD_CREATE_USERID,
	SRC_RCRD_UPD_DTE,
	SRC_RCRD_UPD_USERID,
	RCRD_HASH_KEY,
	DEL_FROM_SRC_FLAG,
	ETL_INS_PID,
	ETL_INS_DTE,
	ETL_UPD_PID,
	ETL_UPD_DTE,
	ZONE3_LOD_DTE,
	EFF_DTE,
	EXPR_DTE,
	CURR_RCRD_FLAG,
	ORP_RCRD_FLAG,
	SLS_ORD_BOM_ID,
	BOM_NBR,
	PRIOR_NODE_NBR,
	PROD_NODE_NBR,
	BOM_CMPNT_SEQ_NBR,
	BOM_LN_ITEM_NBR,
	BOM_CTGY_LKEY,
	RESIDING_LOC_KEY,
	ISS_PLANT_LOC_KEY,
	BOM_CHG_NBR,
	PROD_QTY,
	FIXED_QTY_FLAG,
	OPER_SCRAP_QTY,
	PROD_SCRAP_PCT,
	CMPNT_UOM_KEY,
	ALT_UOM_NUMERAT_VAL,
	ALT_UOM_DENOM_VAL,
	BOM_CMPNT_ITEM_CTGY_LKEY,
	CLS_NBR,
	CLS_TYP_LKEY,
	BOM_CLS_NBR,
	REQ_CMPNT_FLAG,
	COMPATIBLE_UNIT_INSTANCES_NBR,
	BOM_CONFIGURABLE_TYP,
	ORG_FUNC_AREA_NAME,
	PUR_GRP_NAME,
	DOC_TYP_LKEY,
	DOC_NBR,
	DOC_PART_NAME,
	LAST_SHFT_DTE,
	VALID_FROM_DTE_CHG_USER_NAME,
	BOM_PROD_TEXT,
	PROD_ADDTN_DESC,
	PROD_GRP_LKEY,
	PROD_CMPNT_VARIANT_TEXT,
	INTRA_MATL_TEXT,
	ASGN_DEPENDENCY_OBJ_NBR,
	LEADTIME_OFFSET_DYS,
	DLV_TM_IN_DYS,
	UOM_SZ,
	ONE_DIM_VARBL_SZ_1,
	ONE_DIM_VARBL_SZ_2,
	ONE_DIM_VARBL_SZ_3,
	PRODTN_RELVNT_FLAG,
	BOM_SORTING_STRING,
	ITRL_CNTR_NBR,
	PREV_PROD_CNTR,
	PROD_BOM_PRNT_KEY,
	OPERATIONAL_SEQ_NBR,
	BOM_VALID_FROM_DTE,
	BOM_VALID_TO_DTE,
	FIXED_BATCH_SZ_FLAG,
	BY_PROD_FLAG,
	OPTIONAL_PROD_BOM_CMPNT_ID,
	AUXILIARY_UNIT_CODE,
	CONV_RATE,
	BASE_AUXILIARY_QTY,
	PRNT_PROD_TYP,
	PROD_KEY,
	CMPNT_PROD_KEY,
	BOM_TEXT_REF,
	BOM_RELATION_START_DTE,
	BOM_RELATION_END_DTE,
	PROD_STRC_RPT_NBR,
	IS_CMPNT_ITEM_REQ_FLAG,
	QTY_PER_TYP_LKEY,
	PROD_GRP_TYP_LKEY,
	CRTCL_CMPNT_ASSEMBLY_STAT_FLAG,
	PROD_CMPNT_UNIT_VAL,
	PROD_UM_CONV_FACTOR,
	CMDTY_CD_LKEY,
	PROD_BEG_ECN_NBR,
	PROD_END_ECN_NBR,
	JOINT_PROD_TYP_LKEY,
	USER_RMRK_TEXT,
	BATCH_SZ_QTY,
	OPER_STEP_NBR,
	DTE_SHFT_HIER_TYP,
	ALE_STAT_FLAG,
	ALT_PROD_PSTN_NBR,
	MATL_PRVN_TYP,
	AVG_MATL_PURITY_PCT,
	DOC_VER_NBR,
	PROD_EXPLD_TYP_LKEY,
	PUR_ORG_ID,
	USAGE_PRBLTY_PCT,
	PROD_CHG_STAT_ID,
	DOC_HIST_CNTR_NBR,
	EXTNL_PROD_TYP,
	BOM_PROCUR_TYP_LKEY,
	ORIG_SALE_PROD_NODE_NBR,
	ORIG_SALE_PROD_CNTR_NBR,
	PROD_FOLLOW_UP_FLAG,
	BOM_PROD_DEL_FLAG,
	LANG_FMT_TYP_LKEY,
	NEW_SCRAP_INDCTR_FLAG,
	DISCONTINUATION_GRP_NAME,
	FOLLOW_UP_GRP_NAME,
	LEADTIME_OFFSET_UOM,
	OPER_LEADTIME_DY_CNT,
	BOM_PROD_OBJ_GRP_NAME,
	PROD_UNIT_PRC,
	PROD_OBJ_CTGY_LKEY,
	NON_STK_PROD_COST,
	PRODTN_SUPPLY_AREA_LKEY,
	RECURSIVE_BOM_FLAG,
	RECURSIVENESS_CHK_FLAG,
	VARBL_SZ_PROD_TYP_CNT,
	VARBL_SZ_PROD_TOT_QTY,
	RELVNT_PROD_SLS_TYP,
	GL_ACCT_NBR,
	PROD_PLANT_MTNC_FLAG,
	PROD_COST_RELVNT_LKEY,
	ENGNR_RELVNT_PROD_FLAG,
	BULK_MATL_FLAG,
	SUB_PROD_BOM_EXIST_FLAG,
	CMPNT_PRESENT_IN_MULT_ALT_FLAG,
	CRNCY_KEY,
	RCPT_PROC_DY_CNT,
	ALT_PROD_GRP_NBR,
	ALT_PROD_FLAG,
	ALT_PROD_STRAT_LKEY,
	CAD_FLAG,
	ALT_DISPLAY_FMT,
	MULT_SELECTION_ALLOW_FLAG,
	SPARE_PART_TYP,
	FUNC_ID_TYP,
	INSTANCE_FLAG,
	PROD_BOM_CHG_TYP,
	BOM_SLS_ORD_CHG_TYP,
	PROD_BOM_CTGY_LKEY,
	PROD_BOM_SELECTION_TYP,
	SUB_PROD_FLAG,
	VEND_ACCT_NBR,
	FORMULA,
	PROD_BOM_REF_POINT_LKEY,
	PROD_ORIG_CONFIGURATION_FLAG,
	PLANT_MTNC_ASSEMBLY_FLAG,
	TECH_STAT_TEXT,
	PROD_BOM_RESTRICTIONS_FLAG,
	AUTO_CONFIGURATION_DISPLAY_FLAG,
	CONFIGURATION_EDITOR_DISPLAY_FLAG,
	CMPNT_CNSMPTN_DIST_LKEY,
	SINGLE_LVL_CONFIGURATION_DISPLAY_FLAG,
	BOM_SITE,
	BOM_CMPNT_SEGMENTATION,
	SEGMENTATION_VAL,
	BOM_VALID_END_DTE,
	CHG_MSTR_NBR,
	UNLOD_POINT,
	PROD_RCPNT_NAME,
	BOM_INHERIT_NODE_NBR,
	UTC_TM_STAMP,
	SOFTWARE_CMPNT_TYP,
	PROD_BOM_DATA_ELMNT,
	LEN_CALC_MTHD,
	MAX_PRODTN_LEN,
	FIXED_SCRAP_ANY_LEN,
	FIXED_SCRAP_FIRST_LEN,
	FIXED_SCRAP_LAST_LEN,
	RUN_IN_LEN_NBR,
	CMPNT_RND_VAL,
	CMPNT_DEVN_VAL,
	DIST_PRFL_QTY,
	DIST_PRFL_REF_QTY,
	FEASIBILITY_CRTCL_TYP,
	BOM_CMPNT_CRTCL_LVL,
	PRODTN_BOM_NBR,
	VER_NBR,
	TXN_LN_NBR,
	PRODTN_BOM_LKEY,
	BOM_HDR_DESC,
	UOM_NBR,
	ASSEMBLY_ITEM_REQ_QTY,
	INVTY_PLACEMENT_3_PSTN,
	SCRAP_PCT,
	PRODTN_START_DTE,
	PRODTN_END_DTE,
	PROD_PER_UNIT_QTY,
	PROD_CMPNT_TYP_LKEY,
	INVTY_PLACEMENT_1_PSTN,
	INVTY_PLACEMENT_2_PSTN,
	RTG_LINK_CD,
	QTY_CALC_FORMULA,
	CO_NAME,
	BUS_UNIT_KEY,
	PROD_LN_TYP_LKEY,
	BOM_ALT_KIT_NBR,
	ITRL_PROD_NBR,
	BASE_QTY,
	BASE_UOM_KEY,
	PROD_BOM_MSTR_FILE_NBR,
	BOM_KEY,
	ISS_CD_TYP,
	VARBL_QTY,
	BATCH_UNIT_QTY,
	SUBST_ITEM_SEQ_NBR,
	PARTIALS_ALLOW_FLAG,
	REQUIRED,
	OPTIONAL_PROD,
	CMPNT_SEQ_NBR,
	LAST_MTNC_USER_KEY,
	BOM_EFF_DTE,
	PROD_CLS_LKEY,
	LAST_MTNC_DTE,
	SEQ_NBR,
	LAST_MTNC_TM,
	RCRD_ID,
	BOM_WHSE,
	MFG_MTHD_PROD_CD,
	CHILD_ITEM_NBR,
	DSCTU_DTE,
	CHILD_PROD_CLS,
	BOM_SCRAP_FACTOR,
	BUBBLE_NBR,
	OPER_NBR,
	LOWER_LOT_NBR,
	UPPER_LOT_NBR,
	BOM_DYS_OFFSET,
	CO_PROD_COST_PCT,
	MUST_ISS_OVRD,
	CMPNT_USAGE_CD,
	REUSE_OPER,
	BOUNDED_DISPOSITION_CD,
	ORIG_ORD_FAC,
	ORD_TYP,
	ENGNR_CHG_NBR,
	ECN_ORD_LN_NBR,
	REF_DESIGNATORS_FLAG,
	FUTR_CMPNT_FAC,
	PROD_EFF_TM,
	PROD_EFF_TM_ZONE,
	PROD_DSCTU_TM,
	PROD_DSCTU_TM_ZONE,
	TM_ZONE_CREATE,
	TM_ZONE_LAST_CHG,
	LAST_MTNC_PGM,
	EXPR_CMPNT,
	AUTO_RCV_CO_PROD,
	FIXED_LOT_NBR,
	EXCL_COST,
	EXCL_PLNG,
	CMPNT_PROD_ID,
    ALT_BOM_VAL,
   ITEM_SELECTION_COUNTER,
   ALT_BOM_DEL_FLAG,
   CHANGENUMBER,
   INHERITED_BOM_VAL
) as SELECT PROD_BOM_CMPNT_KEY,SRC_SYS_KEY,SRC_RCRD_CREATE_DTE,SRC_RCRD_CREATE_USERID,SRC_RCRD_UPD_DTE,SRC_RCRD_UPD_USERID,RCRD_HASH_KEY,DEL_FROM_SRC_FLAG,ETL_INS_PID,ETL_INS_DTE,ETL_UPD_PID,ETL_UPD_DTE,ZONE3_LOD_DTE,EFF_DTE,EXPR_DTE,CURR_RCRD_FLAG,ORP_RCRD_FLAG,SLS_ORD_BOM_ID,BOM_NBR,PRIOR_NODE_NBR,PROD_NODE_NBR,BOM_CMPNT_SEQ_NBR,BOM_LN_ITEM_NBR,BOM_CTGY_LKEY,RESIDING_LOC_KEY,ISS_PLANT_LOC_KEY,BOM_CHG_NBR,PROD_QTY,FIXED_QTY_FLAG,OPER_SCRAP_QTY,PROD_SCRAP_PCT,CMPNT_UOM_KEY,ALT_UOM_NUMERAT_VAL,ALT_UOM_DENOM_VAL,BOM_CMPNT_ITEM_CTGY_LKEY,CLS_NBR,CLS_TYP_LKEY,BOM_CLS_NBR,REQ_CMPNT_FLAG,COMPATIBLE_UNIT_INSTANCES_NBR,BOM_CONFIGURABLE_TYP,ORG_FUNC_AREA_NAME,PUR_GRP_NAME,DOC_TYP_LKEY,DOC_NBR,DOC_PART_NAME,LAST_SHFT_DTE,VALID_FROM_DTE_CHG_USER_NAME,BOM_PROD_TEXT,PROD_ADDTN_DESC,PROD_GRP_LKEY,PROD_CMPNT_VARIANT_TEXT,INTRA_MATL_TEXT,ASGN_DEPENDENCY_OBJ_NBR,LEADTIME_OFFSET_DYS,DLV_TM_IN_DYS,UOM_SZ,ONE_DIM_VARBL_SZ_1,ONE_DIM_VARBL_SZ_2,ONE_DIM_VARBL_SZ_3,PRODTN_RELVNT_FLAG,BOM_SORTING_STRING,ITRL_CNTR_NBR,PREV_PROD_CNTR,PROD_BOM_PRNT_KEY,OPERATIONAL_SEQ_NBR,BOM_VALID_FROM_DTE,BOM_VALID_TO_DTE,FIXED_BATCH_SZ_FLAG,BY_PROD_FLAG,OPTIONAL_PROD_BOM_CMPNT_ID,AUXILIARY_UNIT_CODE,CONV_RATE,BASE_AUXILIARY_QTY,PRNT_PROD_TYP,PROD_KEY,CMPNT_PROD_KEY,BOM_TEXT_REF,BOM_RELATION_START_DTE,BOM_RELATION_END_DTE,PROD_STRC_RPT_NBR,IS_CMPNT_ITEM_REQ_FLAG,QTY_PER_TYP_LKEY,PROD_GRP_TYP_LKEY,CRTCL_CMPNT_ASSEMBLY_STAT_FLAG,PROD_CMPNT_UNIT_VAL,PROD_UM_CONV_FACTOR,CMDTY_CD_LKEY,PROD_BEG_ECN_NBR,PROD_END_ECN_NBR,JOINT_PROD_TYP_LKEY,USER_RMRK_TEXT,BATCH_SZ_QTY,OPER_STEP_NBR,DTE_SHFT_HIER_TYP,ALE_STAT_FLAG,ALT_PROD_PSTN_NBR,MATL_PRVN_TYP,AVG_MATL_PURITY_PCT,DOC_VER_NBR,PROD_EXPLD_TYP_LKEY,PUR_ORG_ID,USAGE_PRBLTY_PCT,PROD_CHG_STAT_ID,DOC_HIST_CNTR_NBR,EXTNL_PROD_TYP,BOM_PROCUR_TYP_LKEY,ORIG_SALE_PROD_NODE_NBR,ORIG_SALE_PROD_CNTR_NBR,PROD_FOLLOW_UP_FLAG,BOM_PROD_DEL_FLAG,LANG_FMT_TYP_LKEY,NEW_SCRAP_INDCTR_FLAG,DISCONTINUATION_GRP_NAME,FOLLOW_UP_GRP_NAME,LEADTIME_OFFSET_UOM,OPER_LEADTIME_DY_CNT,BOM_PROD_OBJ_GRP_NAME,PROD_UNIT_PRC,PROD_OBJ_CTGY_LKEY,NON_STK_PROD_COST,PRODTN_SUPPLY_AREA_LKEY,RECURSIVE_BOM_FLAG,RECURSIVENESS_CHK_FLAG,VARBL_SZ_PROD_TYP_CNT,VARBL_SZ_PROD_TOT_QTY,RELVNT_PROD_SLS_TYP,GL_ACCT_NBR,PROD_PLANT_MTNC_FLAG,PROD_COST_RELVNT_LKEY,ENGNR_RELVNT_PROD_FLAG,BULK_MATL_FLAG,SUB_PROD_BOM_EXIST_FLAG,CMPNT_PRESENT_IN_MULT_ALT_FLAG,CRNCY_KEY,RCPT_PROC_DY_CNT,ALT_PROD_GRP_NBR,ALT_PROD_FLAG,ALT_PROD_STRAT_LKEY,CAD_FLAG,ALT_DISPLAY_FMT,MULT_SELECTION_ALLOW_FLAG,SPARE_PART_TYP,FUNC_ID_TYP,INSTANCE_FLAG,PROD_BOM_CHG_TYP,BOM_SLS_ORD_CHG_TYP,PROD_BOM_CTGY_LKEY,PROD_BOM_SELECTION_TYP,SUB_PROD_FLAG,VEND_ACCT_NBR,FORMULA,PROD_BOM_REF_POINT_LKEY,PROD_ORIG_CONFIGURATION_FLAG,PLANT_MTNC_ASSEMBLY_FLAG,TECH_STAT_TEXT,PROD_BOM_RESTRICTIONS_FLAG,AUTO_CONFIGURATION_DISPLAY_FLAG,CONFIGURATION_EDITOR_DISPLAY_FLAG,CMPNT_CNSMPTN_DIST_LKEY,SINGLE_LVL_CONFIGURATION_DISPLAY_FLAG,BOM_SITE,BOM_CMPNT_SEGMENTATION,SEGMENTATION_VAL,BOM_VALID_END_DTE,CHG_MSTR_NBR,UNLOD_POINT,PROD_RCPNT_NAME,BOM_INHERIT_NODE_NBR,UTC_TM_STAMP,SOFTWARE_CMPNT_TYP,PROD_BOM_DATA_ELMNT,LEN_CALC_MTHD,MAX_PRODTN_LEN,FIXED_SCRAP_ANY_LEN,FIXED_SCRAP_FIRST_LEN,FIXED_SCRAP_LAST_LEN,RUN_IN_LEN_NBR,CMPNT_RND_VAL,CMPNT_DEVN_VAL,DIST_PRFL_QTY,DIST_PRFL_REF_QTY,FEASIBILITY_CRTCL_TYP,BOM_CMPNT_CRTCL_LVL,PRODTN_BOM_NBR,VER_NBR,TXN_LN_NBR,PRODTN_BOM_LKEY,BOM_HDR_DESC,UOM_NBR,ASSEMBLY_ITEM_REQ_QTY,INVTY_PLACEMENT_3_PSTN,SCRAP_PCT,PRODTN_START_DTE,PRODTN_END_DTE,PROD_PER_UNIT_QTY,PROD_CMPNT_TYP_LKEY,INVTY_PLACEMENT_1_PSTN,INVTY_PLACEMENT_2_PSTN,RTG_LINK_CD,QTY_CALC_FORMULA,CO_NAME,BUS_UNIT_KEY,PROD_LN_TYP_LKEY,BOM_ALT_KIT_NBR,ITRL_PROD_NBR,BASE_QTY,BASE_UOM_KEY,PROD_BOM_MSTR_FILE_NBR,BOM_KEY,ISS_CD_TYP,VARBL_QTY,BATCH_UNIT_QTY,SUBST_ITEM_SEQ_NBR,PARTIALS_ALLOW_FLAG,REQUIRED,OPTIONAL_PROD,CMPNT_SEQ_NBR,LAST_MTNC_USER_KEY,BOM_EFF_DTE,PROD_CLS_LKEY,LAST_MTNC_DTE,SEQ_NBR,LAST_MTNC_TM,RCRD_ID,BOM_WHSE,MFG_MTHD_PROD_CD,CHILD_ITEM_NBR,DSCTU_DTE,CHILD_PROD_CLS,BOM_SCRAP_FACTOR,BUBBLE_NBR,OPER_NBR,LOWER_LOT_NBR,UPPER_LOT_NBR,BOM_DYS_OFFSET,CO_PROD_COST_PCT,MUST_ISS_OVRD,CMPNT_USAGE_CD,REUSE_OPER,BOUNDED_DISPOSITION_CD,ORIG_ORD_FAC,ORD_TYP,ENGNR_CHG_NBR,ECN_ORD_LN_NBR,REF_DESIGNATORS_FLAG,FUTR_CMPNT_FAC,PROD_EFF_TM,PROD_EFF_TM_ZONE,PROD_DSCTU_TM,PROD_DSCTU_TM_ZONE,TM_ZONE_CREATE,TM_ZONE_LAST_CHG,LAST_MTNC_PGM,EXPR_CMPNT,AUTO_RCV_CO_PROD,FIXED_LOT_NBR,EXCL_COST,EXCL_PLNG,CMPNT_PROD_ID,ALT_BOM_VAL,ITEM_SELECTION_COUNTER,ALT_BOM_DEL_FLAG,CHANGENUMBER,INHERITED_BOM_VAL FROM CONSOLIDATED.EDW_PRODUCT_BOM_COMPONENT WHERE CURR_RCRD_FLAG ='Y';
{% endmacro %}