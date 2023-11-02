WITH IH_HIST1 AS (
SELECT DISTINCT UPPER(TRIM(IH_NBR)) AS IH_NBR ,UPPER(TRIM(IH_BOL)) AS IH_BOL FROM {{source("EXCEL_QADDB", "IH_HIST")}} 
WHERE UPPER(TRIM(IH_DOMAIN)) = 'EXCEL'), --AND UPPER(TRIM(IH_NBR)) = 'RG304715')
CTE1 AS ( 
select distinct UPPER(TRIM(TR_TRNBR)) AS TR_NBR,UPPER(TRIM(TR_TYPE)) AS TR_TYPE ,UPPER(TRIM(TR_USERID)) AS TR_USERID,UPPER(TRIM(TR_PROGRAM)) AS TR_PROGRAM,TR_LINE,TR_LOT,UPPER(TRIM(TR_ADDR)) AS TR_ADDR,UPPER(TRIM(TR_TRNBR)) AS TR_TRNBR, 
UPPER(TRIM(TR_SITE)) AS TR_SITE,UPPER(TRIM(TR_LOC)) AS TR_LOC,UPPER(TRIM(TR_PART)) AS TR_PART, TR_DATE,TO_TIME(to_char((TR_TIME))) AS TR_TIME,UPPER(TRIM(TR_REF)) AS TR_REF,LOADDTS,TR_RMKS
FROM {{source("EXCEL_QADDB", "TR_HIST")}} WHERE UPPER(TRIM(TR_DOMAIN)) = 'EXCEL' AND UPPER(TR_TYPE) in ('ISS-SO','ORD-SO'))
select 
UPPER(concat(COALESCE(trim(tr_nbr)::VARCHAR,''),'~',COALESCE(trim(YEAR(tr_date))::VARCHAR,''))) AS INVTY_MVMNT_HDR_KEY,
'EXCEL_QADDB' AS SRC_SYS_KEY,
tr_date AS {{var('column_SRC_RCRD_CREATE_DTE')}},
tr_userid AS {{var('column_SRC_RCRD_CREATE_USERID')}},
tr_date AS {{var('column_SRC_RCRD_UPD_DTE')}},
tr_userid AS {{var('column_SRC_RCRD_UPD_USERID')}}, 
md5(UPPER(concat(COALESCE(trim(tr_nbr)::VARCHAR,''),'~',COALESCE(trim(tr_date)::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS  {{var('column_DEL_FROM_SRC_FLAG')}},  
'{{model.name}}' AS  {{var('column_ETL_INS_PID')}},                                                                  
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS  {{var('column_ETL_UPD_PID')}},                                                                  
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_UPD_DTE')}},
CTE1.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}}, 
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
CTE1.LOADDTS AS {{var('column_z3loddtm')}},
--ptp_det   .ptp_part,ptp_site  AS  RCRD_HASH_KEY,
--tr_hist   .tr_effdate AS  EFF_DTE,
--tr_hist   .tr_expire  AS  EXPR_DTE,
--tr_hist   .tr_date    AS  PROD_MVMNT_DOC_YR,
extract(year from CTE1.tr_date) AS PROD_MVMNT_DOC_YR,
CTE1.tr_type    AS  MVMNT_TYP_LKEY,
CTE1.tr_type    AS  ACCT_DOC_TYP_LKEY,
pl_mstr.pl_inv_sub AS  ACCT_DOC_SUB_TYP_LKEY,
CTE1.tr_date    AS  PROD_MVMNT_DOC_DTE,
CTE1.tr_date    AS  PROD_MVMNT_POST_DTE,
CTE1.tr_date    AS  PROD_MVMNT_DOC_ENTR_DTE,
CTE1.tr_date    AS  PROD_MVMNT_DOC_CHG_DTE,
CTE1.tr_userid  AS  PROD_MVMNT_DOC_UPD_USER_NAME,
CTE1.tr_nbr AS  PRECEDING_DOC_NBR,
CTE1.tr_rmks    AS  PROD_MVMNT_DOC_HDR_TEXT,
IH_HIST1.ih_bol AS  BOL_NBR,
prh_hist.prh_receiver   AS  GOODS_RCPT_NBR,
CTE1.tr_program AS  PROD_MVMNT_DOC_CREATE_PROC_TEXT,
ad_mstr.ad_edi_id  AS  FOREIGN_TRADE_DATA_NBR,
--CTE1.tr_time::varchar AS   GOODS_ISS_TM,
--TO_TIME(to_char((tr_time))) as GOODS_ISS_TM,
case when tr_type like 'ISS%' then TO_TIME(to_char((tr_time)))
 ELSE null END AS GOODS_ISS_TM,
'EXCEL_QADDB' AS GOODS_ISS_SYS_NAME,
CTE1.tr_part AS  PROD_KEY,
CTE1.tr_site AS  LOC_KEY,
ptp_det.ptp_batch   AS  BASE_QTY,
ptp_det.ptp_bom_code    AS  BOM_KEY,
ptp_det.ptp_buyer   AS  PROD_RESP_PRSN_NAME,
ptp_det.ptp_cum_lead    AS  PROD_LEADNUM,
ptp_det.ptp_ins_lead    AS  PUR_INSPCT_NUM,
--ptp_det.ptp_insp_rqd  AS  INCM_INSPCT_FLAG,
CASE WHEN UPPER(TRIM(ptp_det.ptp_ins_rqd)) = 'TRUE' THEN {{var('default_y')}} 
     WHEN UPPER(TRIM(ptp_det.ptp_ins_rqd)) = 'FALSE' THEN {{var('default_n')}} 
     WHEN UPPER(TRIM(ptp_det.ptp_ins_rqd)) is null THEN {{var('default_flag')}}
     ELSE ptp_det.ptp_ins_rqd END AS INCM_INSPCT_FLAG,
CASE WHEN UPPER(TRIM(ptp_det.ptp_iss_pol)) = 'TRUE' THEN {{var('default_y')}} 
     WHEN UPPER(TRIM(ptp_det.ptp_iss_pol)) = 'FALSE' THEN {{var('default_n')}} 
     ELSE ptp_det.ptp_iss_pol END AS PICK_LIST_FLAG,
pt_mstr.pt_mfg_lead AS  PROD_MFG_DY_CNT,
--ptp_det.ptp_ms    AS  PROD_APPEARED_IN_SCHED_FLAG,
CASE WHEN UPPER(TRIM(ptp_det.ptp_ms)) = 'TRUE' THEN {{var('default_y')}} 
     WHEN UPPER(TRIM(ptp_det.ptp_ms)) = 'FALSE' THEN {{var('default_n')}} 
     WHEN UPPER(TRIM(ptp_det.ptp_ms)) is null THEN {{var('default_flag')}}
     ELSE ptp_det.ptp_ms END AS PROD_APPEARED_IN_SCHED_FLAG,
ptp_det.ptp_ord_max AS  PROD_MAX_ORD_QTY,
pt_mstr.pt_ord_min  AS  PROD_MIN_ORD_QTY,
ptp_det.ptp_ord_mult    AS  PROD_LOT_ORD_QTY,
ptp_det.ptp_ord_per AS  ORD_POLICY_DMD_DY_CNT,
ptp_det.ptp_ord_pol AS  MRP_ORD_POLICY_LKEY,
pt_mstr.pt_ord_qty  AS  FOQ_ORD_POLICY_QTY,
--ptp_det.ptp_phantom   AS  PHNTM_FLAG,
CASE WHEN UPPER(TRIM(ptp_det.ptp_phantom)) = 'TRUE' THEN {{var('default_y')}} 
     WHEN UPPER(TRIM(ptp_det.ptp_phantom)) = 'FALSE' THEN {{var('default_n')}} 
     WHEN UPPER(TRIM(ptp_det.ptp_phantom)) is null THEN {{var('default_flag')}}
     ELSE ptp_det.ptp_phantom END AS PHNTM_FLAG,
--ptp_det.ptp_plan_ord  AS  PLND_ORD_FLAG,
CASE WHEN UPPER(TRIM(ptp_det.ptp_plan_ord)) = 'TRUE' THEN {{var('default_y')}} 
     WHEN UPPER(TRIM(ptp_det.ptp_plan_ord)) = 'FALSE' THEN {{var('default_n')}} 
     WHEN UPPER(TRIM(ptp_det.ptp_plan_ord)) is null THEN {{var('default_flag')}}
     ELSE ptp_det.ptp_plan_ord END AS PLND_ORD_FLAG,
ptp_det.ptp_pm_code AS  PROD_PUR_MFG_LKEY,
ptp_det.ptp_pur_lead    AS  CMPLT_PO_DY_CNT,
ptp_det.ptp_rev AS  PROD_ENGNR_RVN_LVL_LKEY,
in_mstr.in_rop  AS  REORD_RPT_QTY,
ptp_det.ptp_routing AS  PROD_RTG_NBR,
CAST(ptp_det.ptp_run AS INT)    AS  PROD_UNIT_STD_RUN_HRS_CNT,
ptp_det.ptp_run_ll  AS  LOWER_LVL_ACCUM_RUN_NUM,
ptp_det.ptp_setup   AS  PROD_SET_UP_STD_NUM,
ptp_det.ptp_setup_ll    AS  LOWER_LVL_ACCUM_SETUP_NUM,
in_mstr.in_sfty_stk AS  PROD_MIN_INVTY_QTY,
ptp_det.ptp_sfty_tme    AS  PRIOR_DYS_FOR_DUE_DTE_CNT,
ptp_det.ptp_timefnce    AS  NO_PLND_ORD_CREATE_DY_CNT,
ptp_det.ptp_vend    AS  PRIM_SUPLR_ADDR_TYP,
ptp_det.ptp_yld_pct AS  PROD_ORD_ACCEPTABLE_PCT,
ptp_det.ptp_assay   AS  PROD_NORMAL_ASSAY_PCT,
in_mstr.in_grade    AS  PROD_INVTY_GRADE_LKEY,
ptp_det.ptp_ll_bom  AS  CALC_SEQ_LKEY,
--ptp_det.ptp_eco_pend  AS  ENGNR_CHG_FLAG,
CASE WHEN UPPER(TRIM(ptp_det.ptp_eco_pend)) = 'TRUE' THEN {{var('default_y')}} 
     WHEN UPPER(TRIM(ptp_det.ptp_eco_pend)) = 'FALSE' THEN {{var('default_n')}} 
     WHEN UPPER(TRIM(ptp_det.ptp_eco_pend)) is null THEN {{var('default_flag')}}
     ELSE ptp_det.ptp_eco_pend END AS ENGNR_CHG_FLAG,
--pt_mstr.pt_rollup AS  BOM_ROLL_UP_FLAG,
CASE WHEN UPPER(TRIM(pt_mstr.pt_rollup)) = 'TRUE' THEN {{var('default_y')}} 
     WHEN UPPER(TRIM(pt_mstr.pt_rollup)) = 'FALSE' THEN {{var('default_n')}} 
     WHEN UPPER(TRIM(pt_mstr.pt_rollup)) is null THEN {{var('default_flag')}}
     ELSE pt_mstr.pt_rollup END AS BOM_ROLL_UP_FLAG,
ptp_det.ptp_added   AS  PROD_SYS_ADD_DTE,
ptp_det.ptp_draw    AS  PROD_DRAWING_ID_LKEY,
pt_mstr.pt_transtype    AS  TRNSPRT_TYP_LKEY,
ptp_det.PTP__CHR01  AS  USER_DEFINE_CUSTOM_1_TEXT,
ptp_det.ptp__chr02  AS  USER_DEFINE_CUSTOM_2_TEXT,
ptp_det.ptp__chr03  AS  USER_DEFINE_CUSTOM_3_TEXT,
ptp_det.ptp__chr04  AS  USER_DEFINE_CUSTOM_4_TEXT,
ptp_det.ptp__chr05  AS  USER_DEFINE_CUSTOM_5_TEXT,
ptp_det.ptp__chr06  AS  USER_DEFINE_CUSTOM_6_TEXT,
ptp_det.ptp__chr07  AS  USER_DEFINE_CUSTOM_7_TEXT,
ptp_det.ptp__chr08  AS  USER_DEFINE_CUSTOM_8_TEXT,
ptp_det.ptp__chr09  AS  USER_DEFINE_CUSTOM_9_TEXT,
ptp_det.ptp__chr10  AS  USER_DEFINE_CUSTOM_10_TEXT,
ptp_det.ptp__dte01  AS  USER_DEFINE_CUSTOM_1_DTE,
ptp_det.ptp__dte02  AS  USER_DEFINE_CUSTOM_2_DTE,
ptp_det.ptp__dec01  AS  USER_DEFINE_CUSTOM_1_NBR,
ptp_det.ptp__dec02  AS  USER_DEFINE_CUSTOM_2_NBR,
ptp_det.ptp__log01  AS  USER_DEFINE_LOGICAL_1_NBR,
ptp_det.ptp__log02  AS  USER_DEFINE_LOGICAL_2_NBR,
ptp_det.ptp_ll_drp  AS  LOWER_LVL_DIST_NETWORK_NBR,
ptp_det.ptp_po_site AS  PUR_LOC_KEY,
ptp_det.ptp_network AS  BOM_NETWORK_NBR,
ptp_det.ptp_mfg_pct AS  MFG_PLND_ORD_PCT,
ptp_det.ptp_pur_pct AS  PUR_PLND_ORD_PCT,
ptp_det.ptp_drp_pct AS  DRP_SUPPLIED_PLND_ORD_PCT,
ptp_det.ptp_pou_code    AS  POINT_OF_USE_LKEY,
ptp_det.ptp_wks_avg AS  FCSTD_AVG_WKS_CNT,
ptp_det.ptp_wks_max AS  FCSTD_MAX_WKS_CNT,
ptp_det.ptp_wks_min AS  FCSTD_MIN_WKS_CNT,
ptp_det.ptp_pick_logic  AS  PICK_LOGIC_LKEY,
CTE1.tr_type AS TXN_CD_TYP,
TO_TIME(to_char((CTE1.tr_time))) as PROD_MVMNT_DOC_ENTR_TM
from CTE1 
LEFT JOIN {{source("EXCEL_QADDB", "PT_MSTR")}} PT_MSTR ON UPPER(TRIM(PT_MSTR.PT_PART)) = UPPER(TRIM(CTE1.TR_PART)) AND UPPER(TRIM(PT_MSTR.PT_DOMAIN)) = 'EXCEL'
LEFT JOIN {{source("EXCEL_QADDB", "PTP_DET")}} PTP_DET ON UPPER(TRIM(PTP_DET.PTP_PART)) = UPPER(TRIM(CTE1.TR_PART)) AND UPPER(TRIM(PTP_DET.PTP_SITE)) = UPPER(TRIM(CTE1.TR_SITE)) AND UPPER(TRIM(PTP_DOMAIN)) = 'EXCEL'--40227426
LEFT JOIN {{source("EXCEL_QADDB", "PRH_HIST")}} PRH_HIST  ON UPPER(TRIM(PRH_HIST.PRH_NBR)) = UPPER(TRIM(CTE1.TR_NBR)) AND UPPER(TRIM(PRH_HIST.PRH_LINE)) = UPPER(TRIM(CTE1.TR_LINE)) AND  
                                                    UPPER(TRIM(PRH_HIST.PRH_PART)) = UPPER(TRIM(CTE1.TR_PART)) AND 
                                                    PRH_HIST.PRH_RECEIVER = CTE1.TR_LOT AND 
													PRH_HIST.PRH_RCP_DATE = CTE1.TR_DATE AND
                                                    UPPER(TRIM(CTE1.TR_TYPE)) IN ('RCT-PO','ISS-PRV') AND UPPER(TRIM(PRH_HIST.PRH_DOMAIN)) = 'EXCEL'
LEFT JOIN {{source("EXCEL_QADDB", "IN_MSTR")}} IN_MSTR ON UPPER(TRIM(IN_MSTR.IN_SITE)) = UPPER(TRIM(CTE1.TR_SITE)) AND UPPER(TRIM(IN_MSTR.IN_PART)) = UPPER(TRIM(CTE1.TR_PART)) AND UPPER(IN_DOMAIN) = 'EXCEL'--40227426
LEFT JOIN {{source("EXCEL_QADDB", "AD_MSTR")}} AD_MSTR ON UPPER(TRIM(AD_MSTR.AD_ADDR)) = UPPER(TRIM(CTE1.TR_ADDR)) AND UPPER(TRIM(AD_MSTR.AD_REF)) = UPPER(TRIM(CTE1.TR_REF)) AND UPPER(TRIM(AD_DOMAIN)) = 'EXCEL'
LEFT JOIN {{source("EXCEL_QADDB", "PL_MSTR")}} PL_MSTR ON UPPER(TRIM(PL_MSTR.PL_PROD_LINE)) = UPPER(TRIM(PT_MSTR.PT_PROD_LINE)) AND UPPER(TRIM(PL_DOMAIN)) = 'EXCEL'
LEFT JOIN IH_HIST1 ON UPPER(TRIM(IH_HIST1.IH_NBR)) = UPPER(TRIM(CTE1.TR_NBR))




-- from {{source("EXCEL_QADDB", "TR_HIST") }} TR_HIST
-- LEFT JOIN {{source("EXCEL_QADDB", "PT_MSTR") }} PT_MSTR ON PT_MSTR.PT_PART = CTE1.TR_PART AND UPPER(PT_MSTR.PT_DOMAIN) = 'EXCEL'--40227426
-- LEFT JOIN {{source("EXCEL_QADDB", "PTP_DET") }} PTP_DET ON PTP_DET.PTP_PART = CTE1.TR_PART AND PTP_DET.PTP_SITE = CTE1.TR_SITE AND UPPER(PTP_DOMAIN) = 'EXCEL'--40227426
-- LEFT JOIN {{source("EXCEL_QADDB", "PRH_HIST") }} PRH_HIST ON PRH_HIST.PRH_NBR =CTE1.TR_NBR AND PRH_HIST.PRH_LINE = CTE1.TR_LINE AND  UPPER(TR_TYPE) IN ('RCT-PO','ISS-PRV') AND PRH_HIST.PRH_DOMAIN = 'EXCEL'
-- LEFT JOIN {{source("EXCEL_QADDB", "IH_HIST") }} IH_HIST  ON IH_HIST.IH_NBR = CTE1.TR_NBR  AND UPPER(TR_TYPE) in ('ISS-SO','ORD-SO') and UPPER(IH_HIST.IH_DOMAIN) = 'EXCEL'
-- LEFT JOIN {{source("EXCEL_QADDB", "IN_MSTR") }} IN_MSTR ON IN_MSTR.IN_SITE = CTE1.TR_SITE AND IN_MSTR.IN_PART = CTE1.TR_PART AND UPPER(IN_DOMAIN) = 'EXCEL'--40227426
-- LEFT JOIN {{source("EXCEL_QADDB", "AD_MSTR") }} AD_MSTR ON AD_MSTR.AD_ADDR = CTE1.TR_ADDR AND UPPER(AD_DOMAIN) = 'EXCEL'--40227426
-- LEFT JOIN {{source("EXCEL_QADDB", "PL_MSTR") }} PL_MSTR ON PL_MSTR.PL_PROD_LINE = PT_MSTR.PT_PROD_LINE AND UPPER(PL_DOMAIN) = 'EXCEL'
-- WHERE UPPER(TRIM(CTE1.TR_DOMAIN)) = 'EXCEL'
