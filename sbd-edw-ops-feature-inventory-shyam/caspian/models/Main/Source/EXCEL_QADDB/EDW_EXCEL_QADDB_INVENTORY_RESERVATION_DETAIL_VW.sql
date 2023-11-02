select 
UPPER(concat(COALESCE(trim(WOD_LOT)::VARCHAR,''),'~',
             COALESCE(trim(WOD_OP)::VARCHAR,''),'~',
              COALESCE(trim(WOD_PART)::VARCHAR,''),'~',
              COALESCE(trim(WOD_DOMAIN)::VARCHAR,''))) AS INVTY_RSRV_DTL_KEY,
'EXCEL_QADDB' AS SRC_SYS_KEY,
wo_mstr.wo_ord_date AS {{var('column_SRC_RCRD_CREATE_DTE')}},
NULL AS {{var('column_SRC_RCRD_CREATE_USERID')}},
NULL AS {{var('column_SRC_RCRD_UPD_DTE')}},
NULL AS {{var('column_SRC_RCRD_UPD_USERID')}}, 
MD5(UPPER(concat(COALESCE(trim(WOD_LOT)::VARCHAR,''),'~',COALESCE(trim(WOD_OP)::VARCHAR,''),'~',COALESCE(trim(WOD_PART)::VARCHAR,''),'~',COALESCE(trim(WOD_DOMAIN)::VARCHAR,'')))) AS RCRD_HASH_KEY,
{{var('default_n')}} AS  {{var('column_DEL_FROM_SRC_FLAG')}},  
'{{model.name}}' AS  {{var('column_ETL_INS_PID')}},  
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS  {{var('column_ETL_UPD_PID')}},  
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_UPD_DTE')}},
WOD_DET.LOADDTS AS {{var('column_vereffdte')}}, 
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}}, 
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
WOD_DET.LOADDTS AS {{var('column_z3loddtm')}},
AD_MSTR.AD_LANG AS  LANG_KEY,
AD_MSTR.AD_ADDR AS  ADDR_CD_KEY,
--LOC_MSTR.LOC_TYPE AS  STORG_TYP_LKEY,
PL_MSTR.PL_INV_ACCT AS  ACCT_ASGN_CTGY_LKEY,
POD_DET.POD_REQ_NBR AS  PUR_REQ_SEQ_NBR,
POD_DET.POD_LINE AS PUR_DOC_SEQ_NBR,
(PS_MSTR.PS_QTY_PER * PS_MSTR.PS_SCRP_PCT)  AS  CMPNT_SCRAP_PCT,
(PS_MSTR.PS_QTY_PER * PS_MSTR.PS_SCRP_PCT)  AS  OPER_SCRAP_QTY,
PS_MSTR.PS_PROCESS  AS  CNSMPTN_POST_LKEY,
PS_MSTR.PS_GROUP AS PROD_GRP_TYP_LKEY,
PT_MSTR.PT_PART AS  PROD_KEY,
PT_MSTR.PT_SNGL_LOT AS  PROD_CMPNT_BATCH_RUN_KEY,
--PT_MSTR.PT_PHANTOM AS PHNTM_PROD_FLAG,
CASE WHEN PT_MSTR.PT_PHANTOM IS NULL THEN 'U' WHEN UPPER(PT_MSTR.PT_PHANTOM) IN ('YES','TRUE','1') THEN 'Y'
WHEN UPPER(PT_MSTR.PT_PHANTOM) IN ('NO','FALSE','0') THEN 'N' ELSE 'U' END AS PHNTM_PROD_FLAG,
PT_MSTR.PT_GROUP    AS  PUR_GRP_LKEY,
--PT_MSTR.PT_PROD_LINE AS   EXTNL_PROCUR_FLAG,
CASE WHEN PT_MSTR.PT_PROD_LINE IS NULL THEN 'U' WHEN UPPER(PT_MSTR.PT_PROD_LINE) IN ('YES','TRUE','1') THEN 'Y'
WHEN UPPER(PT_MSTR.PT_PROD_LINE) IN ('NO','FALSE','0') THEN 'N' ELSE 'U' END AS EXTNL_PROCUR_FLAG,
--PT_MSTR.PT_STATUS AS  PROD_DISCONTINUATION_TYP_LKEY,
CASE WHEN PT_MSTR.PT_STATUS IN  ('IN','OB') THEN PT_STATUS
ELSE NULL END AS PROD_DISCONTINUATION_TYP_LKEY,
CONCAT(PT_MSTR.PT_DESC1,',',PT_MSTR.PT_DESC2)   AS  VARBL_SIZED_PROD_DESC,
PT_MSTR.PT_SIZE_UM  AS  SZ_UOM_KEY,
CONCAT(PT_MSTR.PT_DESC1,',',PT_MSTR.PT_DESC2)   AS  PROD_DESC,
--PTP_DET.PTP_CFG_TYPE AS   IS_PROD_CONFIGURABLE_FLAG,
CASE WHEN PTP_DET.PTP_CFG_TYPE IS NULL THEN 'U' WHEN UPPER(PTP_DET.PTP_CFG_TYPE) IN ('YES','TRUE','1') THEN 'Y'
WHEN UPPER(PTP_DET.PTP_CFG_TYPE) IN ('NO','FALSE','0') THEN 'N' ELSE 'U' END AS IS_PROD_CONFIGURABLE_FLAG,
--PTP_DET.PTP_PLAN_ORD AS   PROD_PLNG_FLAG,
CASE WHEN PTP_DET.PTP_PLAN_ORD IS NULL THEN 'U' WHEN UPPER(PTP_DET.PTP_PLAN_ORD) IN ('YES','TRUE','1') THEN 'Y'
WHEN UPPER(PTP_DET.PTP_PLAN_ORD) IN ('NO','FALSE','0') THEN 'N' ELSE 'U' END AS PROD_PLNG_FLAG,
PTP_DET.PTP_PLAN_ORD AS PLND_ORD_NBR,
--PTP_DET.PTP_POU_CODE AS   PROD_CMPNT_BACKFLUSH_FLAG,
CASE WHEN PTP_DET.PTP_POU_CODE IS NULL THEN 'U' WHEN UPPER(PTP_DET.PTP_POU_CODE) IN ('YES','TRUE','1') THEN 'Y'
WHEN UPPER(PTP_DET.PTP_POU_CODE) IN ('NO','FALSE','0') THEN 'N' ELSE 'U' END AS PROD_CMPNT_BACKFLUSH_FLAG,
PTP_DET.PTP_BOM_CODE AS BOM_EXPLD_NBR,
PTP_DET.PTP_BOM_CODE AS BOM_NBR,
PTP_DET.PTP_SITE AS PROD_SLOC_KEY,
PTP_DET.PTP_ATP_ENFORCEMENT AS  ATP_STRC_NODE,
PTP_DET.PTP_BOM_CODE AS PROD_BOM_NBR,
PTP_DET.PTP_VEND AS VEND_ID,
--TR_HIST.TR_LINE   AS  SLS_ORD_LN_1_KEY,
CASE WHEN UPPER(TRIM(TR_HIST.TR_TYPE)) ='ISS_SO' THEN CAST(TR_LINE AS VARCHAR)
ELSE TR_HIST.TR_TYPE END AS SLS_ORD_LN_1_KEY,
TR_HIST.TR_UM   AS  DFLT_QTY_UOM_KEY,
TR_HIST.TR_UM   AS  RESERVED_QTY_UOM_KEY,
--TR_HIST.TR_TYPE   AS  RECORDED_PLANT_LOC_KEY,
CASE WHEN UPPER(TRIM(TR_HIST.TR_TYPE)) LIKE '%ISS' THEN 'ISSUED'
 WHEN UPPER(TRIM(TR_HIST.TR_TYPE)) LIKE '%RCT' THEN 'RECEIVED'
ELSE TR_HIST.TR_TYPE END RECORDED_PLANT_LOC_KEY,
TR_HIST.TR_CURR AS  STORG_WITHDRAWN_VAL_CRNCY_KEY,
TR_HIST.TR_ADDR AS  SHIPTO_CUST_KEY,
TR_HIST.TR_TYPE AS  RCRD_TYP,
--TR_HIST.TR_TYPE   AS  MVMNT_TYP_LKEY
CASE WHEN TR_HIST.TR_TYPE LIKE '%WO' THEN TR_TYPE ELSE NULL END AS MVMNT_TYP_LKEY,
--TR_HIST.TR_NBR AS PO_NBR
CASE WHEN UPPER(TRIM(TR_HIST.TR_TYPE)) IN ('RCT-PO','ISS-PRV','ISS-RV','ORD-PO') THEN TR_NBR
ELSE TR_HIST.TR_TYPE END AS PO_NBR,
--TR_HIST.TR_SHIRT AS   SHORTFALL_QTY,
--TR_HIST.TR_CURR   AS  CMPNT_CRNCY_PRC,
TR_HIST.TR_NBR  AS  SLS_ORD_HDR_KEY,
TR_HIST.TR_LINE AS  SLS_ORD_LN_2_KEY,
TR_HIST.TR_UM   AS  OPER_OFFSET_LEADTIME_UOM_KEY,
--TR_HIST.TR_XCR_ACCOUNT    AS  DEBIT_CREDIT_LKEY, column not available
--TR_HIST.TR_XDR_ACCOUNT AS DEBIT_CREDIT_LKEY,
--CASE WHEN TR_HIST.TR_XCR_ACCOUNT IS NOT NULL THEN 'H' ELSE CASE WHEN TR_HIST.TR_XDR_ACCOUNT IS NOT NULL THEN 'S' ELSE 'O' END END AS DEBIT_CREDIT_LKEY,
--if tr_xcr_account not null the "H" else if tr_Xdr_account is not null the "S"
TR_HIST.TR_PER_DATE AS  SVC_PERFORMER_NAME,
TR_HIST.TR_TRNBR    AS  PO_DOC_NBR,
TR_HIST.TR_LOC  AS  UNLOD_POINT_NBR,
TR_HIST.TR_LINE AS  SLS_DLV_SCHED_LN_NBR,
TR_HIST.TR_SITE AS  WHSE_LOC_NBR,
TR_HIST.TR_ENDUSER  AS  SHIPTO_CUST_NBR,
--WO_MSTR.WO_ORD_DATE   AS  SRC_RCRD_CREATE_DTE,
--WO_MSTR.WO_EXPIRE AS  EXPR_DTE,
WO_MSTR.WO_NBR  AS  INVTY_RSRV_HDR_KEY,
WO_MSTR.WO_ASSAY    AS  HIGHER_LVL_PROD_KEY,
--WO_MSTR.WO_DUE_DATE   AS  PROD_REQ_DTE,
--(WO_MSTR.WO_SETUP_TIME,WO_MSTR.WO_DURATION_BUFFER) AS PROD_REQ_TM,
WO_MSTR.WO_BATCH    AS  PROD_LOT_BATCH_NBR,
WO_MSTR.WO_BATCH    AS  ITRL_OBJ_NBR,
WO_MSTR.WO_ACCT AS  FUNC_AREA_TEXT,
--WO_MSTR.WO_JOINT_TYPE AS  IS_FIXED_PRC_CO_PROD_FLAG
CASE WHEN UPPER(WO_MSTR.WO_JOINT_TYPE) IS NULL THEN 'U' WHEN UPPER(WO_MSTR.WO_JOINT_TYPE) IN ('YES','TRUE','1') THEN 'Y'
WHEN UPPER(WO_MSTR.WO_JOINT_TYPE) IN ('NO','FALSE','0') THEN 'N' ELSE 'U' END AS IS_FIXED_PRC_CO_PROD_FLAG,
WO_MSTR.WO_SUB  AS  SUBDIV_COST_ELMNT_ORIG_GRP_TEXT,
WO_MSTR.WO_BATCH AS COND_RCRD_NBR,
--WO_MSTR.WO_ACCT_CLOSE AS  RSRV_FINAL_ISS_FLAG,
CASE WHEN UPPER(WO_MSTR.WO_ACCT_CLOSE) IS NULL THEN 'U' WHEN UPPER(WO_MSTR.WO_ACCT_CLOSE) IN ('YES','TRUE','1') THEN 'Y'
WHEN UPPER(WO_MSTR.WO_ACCT_CLOSE) IN ('NO','FALSE','0') THEN 'N' ELSE 'U' END AS RSRV_FINAL_ISS_FLAG,
--WO_MSTR.WO_JOINT_TYPE AS  CO_PROD_FLAG,
CASE WHEN UPPER(WO_MSTR.WO_JOINT_TYPE) IS NULL THEN 'U' WHEN UPPER(WO_MSTR.WO_JOINT_TYPE) IN ('co_product','TRUE','1') THEN 'Y'
WHEN UPPER(WO_MSTR.WO_JOINT_TYPE) IN ('','FALSE','0') THEN 'N' ELSE 'U' END AS CO_PROD_FLAG,
WO_MSTR.WO_LEAD_TIME    AS  LEADTIME_OFFSET_DY_CNT,
WO_MSTR.WO_ACCT AS  GL_ACCT_NBR,
WO_MSTR.WO_ROUTING  AS  RTG_SEQ_NBR,
WR_ROUTE.WR_SUB_LEAD AS OPER_OFFSET_LEADTIME_CNT,
WOD_DET.WOD_NBR AS  PROD_RSRV_NBR,
WOD_DET.WOD_LOT AS  PROD_RSRV_LN_NBR,
WOD_DET.WOD_QTY_REQ AS  USAGE_QTY,
--LOC_MSTR.LOC_LOC  AS  INVTY_SLOC_KEY,
WOD_DET.WOD_REF AS  STORG_SUB_BIN_NBR,
WOD_DET.WOD_NBR AS  RSRV_ORD_NBR,
WO_MSTR.WO_ROUTING  AS  OPER_RTG_NBR,
WOD_DET.WOD_ISS_DATE AS PROD_REQ_DTE,
WOD_DET.WOD_YIELD_PCT AS    PROD_USAGE_PRBLTY_PCT,
WOD_DET.WOD_STD_COST AS LCRNCY_FIXED_PRC,
WOD_DET.WOD_STD_COST AS CMPNT_CRNCY_PRC,
WOD_DET.WOD_TOT_STD AS  LCRNCY_TOT_PRC,
WOD_DET.WOD_DOMAIN  AS  BUS_AREA_LKEY,
WOD_DET.WOD_QTY_REQ AS  PROD_REQ_QTY,
WOD_DET.WOD_QTY_REQ AS  REQ_QTY,
WOD_DET.WOD_NBR AS  OBJ_NBR,
WOD_DET.WOD_REF AS  STORG_BIN_NBR,
WOD_DET.WOD_NBR AS  ORD_PROD_DOC_NBR,
WOD_DET.WOD_CMTINDX AS  FREE_TEXT,
WOD_DET.WOD_PART    AS  RSRV_REQ_PROD_NBR,
WOD_DET.WOD_QTY_CHG AS  RQST_TRNSFR_QTY,
WOD_DET.WOD_SITE AS PLANT_LOC_KEY,
WOD_DET.WOD_DOMAIN  AS  DOMAIN_CD,
WOD_DET.WOD_PART    AS  INVTY_PROD_CD,
WOD_DET.WOD_QTY_ALL AS  INVTY_RSRV_QTY,
WOD_DET.WOD_QTY_CHG AS  TOT_INVTY_TRNSFR_QTY,
WOD_DET.WOD_QTY_ALL AS  TOT_INVTY_ALLOC_QTY,
--WOD_DET.WOD_CRITICAL  AS  FEASIBILITY_CRTCL_FLAG,
CASE WHEN UPPER(WOD_DET.WOD_CRITICAL) IS NULL THEN 'U' WHEN UPPER(WOD_DET.WOD_CRITICAL) IN ('YES','TRUE','1') THEN 'Y'
WHEN UPPER(WOD_DET.WOD_CRITICAL) IN ('NO','FALSE','0') THEN 'N' ELSE 'U' END AS FEASIBILITY_CRTCL_FLAG,
YEAR(WOD_DET.WOD_ISS_DATE) AS INVTY_ALLOC_YR,
MONTH(WOD_DET.WOD_ISS_DATE) AS INVTY_ALLOC_MTH 
from {{source("EXCEL_QADDB", "WOD_DET")}} WOD_DET
LEFT JOIN {{source("EXCEL_QADDB", "WO_MSTR")}} WO_MSTR ON UPPER(TRIM(WO_MSTR.WO_DOMAIN)) = 'EXCEL' AND UPPER(TRIM(WOD_DET.WOD_LOT)) = UPPER(TRIM(WO_MSTR.WO_LOT)) AND UPPER(TRIM(WOD_DET.WOD_ITM_LINE)) = UPPER(TRIM(WO_MSTR.WO_ITM_LINE)) --2,695,827
LEFT JOIN {{source("EXCEL_QADDB", "MRP_DET")}} MRP_DET ON UPPER(TRIM(MRP_DET.MRP_DOMAIN)) = 'EXCEL' AND UPPER(TRIM(WOD_DET.WOD_PART)) = UPPER(TRIM(MRP_DET.MRP_PART)) AND  UPPER(TRIM(WOD_DET.WOD_NBR)) = UPPER(TRIM(MRP_DET.MRP_NBR)) AND UPPER(TRIM(MRP_DET.MRP_LINE)) =  UPPER(TRIM(WOD_DET.WOD_LOT)) AND  UPPER(TRIM(MRP_DET.MRP_LINE2)) =UPPER(TRIM(CAST(WOD_OP AS VARCHAR))) AND UPPER(TRIM(MRP_DET.MRP_DATASET)) = UPPER(TRIM('wod_det')) --2,695,827
LEFT JOIN {{source("EXCEL_QADDB", "POD_DET")}} POD_DET ON UPPER(TRIM(POD_DET.POD_DOMAIN)) = 'EXCEL' AND  UPPER(TRIM(MRP_DET.MRP_PART)) = UPPER(TRIM(POD_DET.POD_PART)) AND UPPER(TRIM(POD_DET.POD_NBR)) = UPPER(TRIM(MRP_DET.MRP_NBR)) AND UPPER(TRIM(MRP_DET.MRP_LINE)) = UPPER(TRIM(POD_DET.POD_LINE)) AND  UPPER(TRIM(MRP_DET.MRP_DATASET)) = UPPER(TRIM('pod_det')) --2,695,827
LEFT JOIN {{source("EXCEL_QADDB", "PO_MSTR")}} PO_MSTR ON UPPER(TRIM(PO_MSTR.PO_DOMAIN)) = 'EXCEL' AND UPPER(TRIM(POD_DET.POD_NBR)) = UPPER(TRIM(PO_MSTR.PO_NBR)) --2,695,827
LEFT JOIN {{source("EXCEL_QADDB", "PT_MSTR")}} PT_MSTR ON UPPER(TRIM(PT_MSTR.PT_DOMAIN)) = 'EXCEL' AND UPPER(TRIM(WOD_DET.WOD_STATUS)) IN ('F','E') AND  UPPER(TRIM(WOD_DET.WOD_PART)) = UPPER(TRIM(PT_MSTR.PT_PART)) --2,695,827
LEFT JOIN {{source("EXCEL_QADDB", "PS_MSTR")}} PS_MSTR ON UPPER(TRIM(PS_MSTR.PS_DOMAIN)) = 'EXCEL' AND UPPER(TRIM(PT_MSTR.PT_PART)) = UPPER(TRIM(PS_MSTR.PS_COMP)) --2,695,827
LEFT JOIN {{source("EXCEL_QADDB", "PTP_DET")}} PTP_DET ON  UPPER(TRIM(PTP_DET.PTP_DOMAIN)) = 'EXCEL' AND UPPER(TRIM(WOD_DET.WOD_STATUS)) IN ('F','E') AND UPPER(TRIM(WOD_DET.WOD_PART)) =  UPPER(TRIM(PTP_DET.PTP_PART)) AND UPPER(TRIM(WOD_DET.WOD_SITE)) = UPPER(TRIM(PTP_DET.PTP_SITE)) 
LEFT JOIN (SELECT * FROM {{source("EXCEL_QADDB", "TR_HIST")}} QUALIFY ROW_NUMBER() OVER (PARTITION BY TR_SITE,TR_LOC,TR_PART,TR_REF
ORDER BY TR_SITE,TR_LOC,TR_PART,TR_REF,TR_DATE,TR_TIME DESC) =1) TR_HIST ON  UPPER(TRIM(TR_HIST.TR_DOMAIN)) = 'EXCEL'  
AND UPPER(TRIM(WOD_DET.WOD_NBR)) =  UPPER(TRIM(TR_HIST.TR_NBR)) AND UPPER(TRIM(WOD_DET.WOD_PART)) = UPPER(TRIM(TR_HIST.TR_PART)) AND UPPER(TRIM(WOD_DET.WOD_LOT)) = UPPER(TRIM(TR_HIST.TR_LOT)) AND UPPER(TRIM(TR_HIST.TR_TYPE)) = 'ISS-WO' --2695827
LEFT JOIN {{source("EXCEL_QADDB", "AD_MSTR")}} AD_MSTR ON  UPPER(TRIM(AD_MSTR.AD_DOMAIN)) = 'EXCEL' AND UPPER(TRIM(TR_HIST.TR_ADDR)) = UPPER(TRIM(AD_MSTR.AD_ADDR))  
LEFT JOIN {{source("EXCEL_QADDB", "PL_MSTR")}} PL_MSTR  ON UPPER(TRIM(PL_MSTR.PL_DOMAIN)) = 'EXCEL' AND  UPPER(TRIM(PT_MSTR.PT_PROD_LINE)) = UPPER(TRIM(PL_MSTR.PL_PROD_LINE)) --2695827
LEFT JOIN {{source("EXCEL_QADDB", "WR_ROUTE")}} WR_ROUTE ON UPPER(TRIM(WR_ROUTE.WR_DOMAIN)) = 'EXCEL' AND UPPER(TRIM(WO_MSTR.WO_LOT)) = UPPER(TRIM(WR_ROUTE.WR_LOT)) AND UPPER(TRIM(WOD_DET.WOD_OP)) = UPPER(TRIM(WR_ROUTE.WR_OP)) WHERE UPPER(TRIM(WOD_DET.WOD_DOMAIN)) = 'EXCEL' --2695827