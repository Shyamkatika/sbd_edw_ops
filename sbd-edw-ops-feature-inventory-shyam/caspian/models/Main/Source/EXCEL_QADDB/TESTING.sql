SELECT 
UPPER(concat(COALESCE(trim(tr_trnbr)::VARCHAR,''),'~',COALESCE(trim(YEAR(tr_date))::VARCHAR,''),'~',COALESCE(trim(tr_line)::VARCHAR,''))) AS INVTY_MVMNT_LN_KEY,
'EXCEL_QADDB' AS {{var('column_srcsyskey')}},
tr_date AS {{var('column_SRC_RCRD_CREATE_DTE')}},
tr_userid AS {{var('column_SRC_RCRD_CREATE_USERID')}},
tr_date AS {{var('column_SRC_RCRD_UPD_DTE')}},
tr_userid AS {{var('column_SRC_RCRD_UPD_USERID')}}, 
md5(UPPER(concat(COALESCE(trim(tr_trnbr)::VARCHAR,''),'~',COALESCE(trim(tr_date)::VARCHAR,''),'~',COALESCE(trim(tr_line)::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS  {{var('column_DEL_FROM_SRC_FLAG')}},  
'{{model.name}}' AS  {{var('column_ETL_INS_PID')}},                                                                  
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS  {{var('column_ETL_UPD_PID')}},                                                                  
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_UPD_DTE')}},
TR_HIST.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}}, 
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
TR_HIST.LOADDTS AS {{var('column_z3loddtm')}},
TR_HIST.TR_LINE	AS	PROD_ORD_LN_NBR,
TR_HIST.TR_GL_AMT	AS	ACCT_VAL_LKEY,
TR_HIST.TR_UM	AS	PUR_DLV_QTY_UOM_KEY,
TR_HIST.TR_TYPE	AS	VAL_TEXT,
TR_HIST.TR_LINE	AS	ACCT_DOC_LN_1_NBR,
TR_HIST.TR_LINE	AS	ACCT_DOC_LN_2_NBR,
TR_HIST.TR_TYPE	AS	STK_MVMNT_TYP_LKEY,
ds_det.ds_order_category AS WHSE_MVMNT_TYP_LKEY,
TR_HIST.TR_BATCH    AS  BATCH_NBR_LKEY,
  CASE WHEN UPPER(TRIM(TR_HIST.TR_REF)) = 'BIN' THEN {{var('default_y')}} 
  ELSE {{var('default_n')}}  END AS DYN_STORG_BIN_FLAG,
  CASE WHEN UPPER(TRIM(prh_hist.prh_cst_up)) = 'TRUE' THEN {{var('default_y')}} 
     WHEN UPPER(TRIM(prh_hist.prh_cst_up)) = 'FALSE' THEN {{var('default_n')}} 
	 WHEN UPPER(TRIM(prh_hist.prh_cst_up)) is null THEN {{var('default_flag')}}
	 ELSE prh_hist.prh_cst_up END AS GOODS_RCPT_VALUATED_FLAG,
  CASE WHEN UPPER(TRIM(prh_hist.prh_rcp_type)) = 'TRUE' THEN {{var('default_y')}} 
     WHEN UPPER(TRIM(prh_hist.prh_rcp_type)) = 'FALSE' THEN {{var('default_n')}} 
	 WHEN UPPER(TRIM(prh_hist.prh_rcp_type)) is null THEN {{var('default_flag')}}
	 ELSE prh_hist.prh_rcp_type END AS GOOD_RCPT_REVERSAL_FLAG,	 
dsr_mstr.dsr_part AS TRNSFR_REQ_PRYRT_NBR,
dsr_mstr.dsr_site AS WHSE_NBR,
dsr_mstr.dsr_site as WHSE_LKEY,
tr_hist.tr_user1 as PRSN_NBR,
tr_hist.tr_msg AS  RESN_CD_LKEY,
TR_HIST.TR_TYPE AS  PO_LN_NBR ,
TR_HIST.TR_UM   AS  ENTR_UNIT_QTY_UOM_KEY,
TR_HIST.TR_QTY_CHG  AS  ENTR_UNIT_QTY,
TR_HIST.TR_RMKS AS  MVMNT_REASONS_TYP_LKEY,
TR_HIST.TR_ADDR AS  CUST_KEY,
TR_HIST.TR_TYPE AS  STK_MVMNT_RESN_LKEY,
TR_HIST.TR_REF  AS  REF_DOC_NBR,
TR_HIST.TR_REF  AS  WHSE_STORG_SUB_LOC_NAME,
TR_HIST.TR_PART AS  PROD_KEY,
CASE WHEN tr_type like 'ISS%' THEN tr_part ELSE tr_type END AS RCV_ISS_PROD_ID,
TR_HIST.TR_UM   AS  PROD_BASE_UOM_KEY,
TR_HIST.TR_QTY_CHG    AS  GOODS_RCPT_ISS_PROD_QTY,
TR_HIST.TR_QTY_CHG  AS  ACCT_ASGN_PROD_QTY,
TR_HIST.TR_XCR_PROJ AS  PROJ_NBR,
TR_HIST.TR_STATUS   AS  GOOD_RCPT_INSPCT_STAT_LKEY,
TR_HIST.TR_TYPE AS  RCRD_TYP_TEXT,
TR_HIST.TR_XDR_ACCT     AS  GL_ACCT_NBR,
TR_HIST.TR_RMKS AS  FREE_TEXT,
dsr_mstr.dsr_req_nbr AS TRNSFR_REQ_NBR,
CONCAT(dsr_mstr.dsr_req_nbr,dsr_mstr.dsr_part) AS TRNSFR_REQ_SEQ_NBR,
(idh_hist.idh_qty_inv * idh_hist.idh_price) AS SLS_EXCLUDED_TAX_PRC,
(idh_hist.idh_qty_inv * idh_hist.idh_price) * (Tx2_tax_pct*100) as SLS_INCLD_TAX_PRC,
TR_HIST.TR_PROGRAM  AS  INVTY_MVMNT_LN_CREATE_PROC_TEXT,
TR_HIST.TR_BATCH    AS  RCV_ISS_BATCH_NBR,
TR_HIST.TR_TYPE AS  RCV_ISS_PLANT_LOC_KEY,
TR_HIST.TR_USERID   AS  TXN_USER_NAME,
TR_HIST.TR_NBR  AS  SLS_DLV_NBR,
--TR_HIST.TR_TYPE AS  SLS_DLV_ITEM_NBR,
TR_HIST.TR_TYPE AS  MVMNT_TYP_LKEY,
TR_HIST.TR_CURR AS  CRNCY_KEY,
TR_HIST.TR_SITE AS  PLANT_LOC_KEY,
TR_HIST.TR_LINE AS  PROD_MVMNT_DOC_LN_NBR,
TR_HIST.TR_XDR_ACCT AS  DOC_ACCT_1_NBR,
TR_HIST.TR_XCR_ACCT AS  DOC_ACCT_2_NBR,
TR_HIST.TR_ENDUSER  AS  PROC_USER_NAME_TEXT,
TR_HIST.TR_SITE AS  LOC_KEY,
TR_HIST.TR_TYPE AS  TXN_TYP_LKEY,
TR_HIST.TR_TYPE AS  PROD_MVMNT_REF_TYP_LKEY,
TR_HIST.TR_NBR  AS  PROD_MVMNT_REF_NBR,
TR_HIST.TR_LINE AS  PROD_MVMNT_REF_LN_NBR,
TR_HIST.TR_REF_SITE AS  DEST_REF_DOC_NBR,
TR_HIST.TR_PROGRAM  AS  WORK_CNTR_NAME,
TR_HIST.TR_LBR_STD  AS  LABR_COST,
TR_HIST.TR_OVH_STD  AS  OVER_HEAD_COST,
TR_HIST.TR_OVH_STD  AS  VARBL_OVER_HEAD_COST,
TR_HIST.TR_SO_JOB   AS  INVTY_DOC_NBR,
TR_HIST.TR_TRNBR    AS  RPT_ENTR_NBR,
TR_HIST.TR_SVC_TYPE AS  SVC_DOC_TYP,
TR_HIST.TR_PART AS  SVC_DOC_ITEM_NBR,
TR_HIST.TR_TRNBR    AS  GOODS_MVMNT_DOC,
TR_HIST.TR_UM   AS  PARALLEL_UOM,
TR_HIST.TR_UM   AS  PUOM_ENTR_UOM,
TR_HIST.TR_UM   AS  GOODS_RCPT_QTY_UOM_NBR,
TR_HIST.TR_NBR  AS  PO_NBR ,
TR_HIST.TR_PART AS  MFGR_PROD_ID,
TR_HIST.TR_NBR  AS  SLS_ORD_DLV_SCHED_LN_NBR,
TR_HIST.TR_WOD_OP   AS  SLS_ORD_STK_WBS_NBR,
TR_HIST.TR_TRNBR    AS  TRNSFR_ORD_NBR,
--TR_HIST.TR_PART AS  RCV_ISS_PROD_ID,
TR_HIST.TR_NBR  AS  SLS_ORD_NBR,
CASE WHEN tr_type like 'ISS%' THEN tr_loc ELSE tr_type END AS RCV_ISS_SLOC_ID,
TR_HIST.TR_UM   AS  DLV_UOM_KEY,
AR_MSTR.AR_TYPE AS FIN_CTRL_AREA_ACTVTY_LKEY,
AD_MSTR.AD_CTRY AS ENTY_CNTRY_KEY,
pt_mstr.pt_run_seq1 AS REQ_SEG,
pl_mstr.pl_inv_acct AS ACCT_ASGN_CTGY,
PTP_DET.PTP_ROUTING AS PRODTN_ORD_RTG_NBR,
PTP_DET.PTP_PLAN_ORD AS PRODTN_PLNG_PROD_STG_LKEY,
PTP_DET.PTP_VEND AS VEND_ACCT_NBR,
PTP_DET.PTP_ORD_PER AS UOM_QTY,
PTP_DET.PTP_NETWORK AS PROJ_NETWORK_NBR,
case when PTP_DET.PTP_POU_CODE is not null then 'Y' else 'N' end AS BACKFLUSH_FLAG,
PS_MSTR.PS_PROCESS AS CNSMPTN_POST_LKEY,
PO_MSTR.PO_CMTINDX AS SHIP_INSTRC_LKEY,
PRH_HIST.PRH_PS_QTY	AS PUR_DLV_QTY,
PRH_HIST.PRH_RCVD	AS GOODS_RCPT_QTY,
PRH_HIST.PRH_PS_NBR	AS SENDER_DOC_NBR,
PRH_HIST.PRH_VEND	AS SENDER_PAYER,
PRH_HIST.PRH_PS_NBR	AS GOODS_RCPT_SLIPS_PRINTED_CNT,
sct_det.sct_cst_tot	AS DLV_LCRNCY_COST,
sct_det.sct_cst_tot	AS LCRNCY_AMT,
sct_det.sct_cst_tot	AS EXTNL_POST_LCRNCY_AMT,
in_mstr.in_loc_type	AS STK_TYP_MOD_LKEY,
sct_det.sct_cst_tot	AS PROD_MVMNT_COST,
sct_det.sct_cst_tot	AS PROD_COST,
ih_hist.ih_cust	AS SENDER_CUST_NAME,
lad_det.lad_lot AS LOT_NBR,
si_mstr.si_entity AS CO_KEY,
si_mstr.si_entity AS FIN_PROFIT_CNTR_ID,
loc_mstr.loc_type AS STORG_TYP_LKEY,
loc_mstr.loc_loc	AS SLOC_ID,
wo_mstr.wo_part	AS WORK_ITEM_NBR,
tx2_mstr.TX2_TAX_TYPE AS TAX_JURSDN_RATE,
wo_mstr.wo_nbr AS WORK_BRKDN_STRC_NBR,
 extract(year from tr_hist.tr_date) AS	PROD_MVMNT_DOC_LN_NBR_YR,
 TR_HIST.TR_GL_DATE  AS  FIN_ACCT_DTE,
 --TO_TIME(to_char((TR_HIST.tr_time)))  AS  ACCT_DOC_DTE_ENTR_TM
TR_HIST.tr_time::timestamp_ntz as ACCT_DOC_DTE_ENTR_TM,
TR_HIST.tr_time::timestamp_ntz  AS  ENTR_TM,
TR_HIST.TR_NBR  AS  VALUATED_STK_SLS_ORD_NBR,
case when tr_hist.tr_type like 'ISS%' then tr_part else tr_hist.tr_type end SLS_DLV_ITEM_NBR,
pt_mstr.pt_shelflife::timestamp_ntz as SHELF_LIFE_EXPR_DTE,
TR_HIST.TR_DATE as PROD_MVMNT_DTE,
TR_HIST.TR_PER_DATE AS  SVC_PERFORMER,
extract(year from tr_hist.tr_date) AS  PROD_MVMNT_DOC_YR,
CASE WHEN TR_HIST.TR_TYPE = 'CUM-RRES' THEN TR_HIST.tr_qty_chg ELSE 0 END AS CUML_RCV_QTY,
TR_HIST.tr_time::timestamp_ntz as SVCS_RENDERED_DTE,
VD_MSTR.VD_SORT AS SENDER_VEND_NAME,
tr_hist.tr_qty_chg AS PARALLEL_UOM_QTY,
VD_MSTR.VD_ADDR AS VEND_TO_ID,
CASE WHEN tr_type = 'ISS-SO' THEN TR_HIST.TR_LINE ELSE 0 END  AS  SLS_ORD_LN_NBR
FROM "DEV_RAW"."EXCEL_QADDB"."VW_TR_HIST"  TR_HIST --40227426
LEFT JOIN "DEV_RAW"."EXCEL_QADDB"."VW_AD_MSTR" AD_MSTR ON AD_MSTR.AD_ADDR =   TR_HIST.TR_ADDR AND AD_DOMAIN = 'EXCEL'--40227426
LEFT JOIN "DEV_RAW"."EXCEL_QADDB"."VW_AR_MSTR" AR_MSTR ON AR_MSTR.AR_SO_NBR = TR_HIST.TR_NBR  AND AR_MSTR.AR_NBR  =   TR_HIST.TR_RMKS AND UPPER(AR_DOMAIN) = 'EXCEL'AND  AR_TYPE = 'I'--40227426
LEFT JOIN "DEV_RAW"."EXCEL_QADDB"."VW_PT_MSTR" PT_MSTR ON PT_MSTR.PT_PART = TR_HIST.TR_PART AND PT_MSTR.PT_DOMAIN = 'EXCEL'--40227426
LEFT JOIN "DEV_RAW"."EXCEL_QADDB"."VW_PL_MSTR" PL_MSTR ON PL_MSTR.PL_PROD_LINE = PT_MSTR.PT_PROD_LINE AND PL_DOMAIN = 'EXCEL'--40227426
LEFT JOIN "DEV_RAW"."EXCEL_QADDB"."VW_PTP_DET" PTP_DET ON PTP_DET.PTP_PART = TR_HIST.TR_PART AND PTP_DET.PTP_SITE = TR_HIST.TR_SITE AND PTP_DOMAIN = 'EXCEL'--40227426
LEFT JOIN "DEV_RAW"."EXCEL_QADDB"."VW_PS_MSTR" PS_MSTR ON PS_MSTR.PS_COMP = PT_MSTR.PT_PART AND PS_DOMAIN = 'EXCEL'--40227426
LEFT JOIN "DEV_RAW"."EXCEL_QADDB"."VW_POD_DET" POD_DET ON POD_DET.POD_NBR = TR_HIST.TR_NBR AND POD_DET.POD_LINE = TR_HIST.TR_LINE and TR_TYPE in('RCT-PO','ISS-PRV')--40227426
LEFT JOIN "DEV_RAW"."EXCEL_QADDB"."VW_PO_MSTR" PO_MSTR ON PO_MSTR.PO_NBR = TR_HIST.TR_NBR--40227426
left join (select prh_receiver,PRH_NBR,PRH_LINE,PRH_PS_QTY,PRH_RCVD,PRH_PS_NBR,PRH_VEND,PRH_DOMAIN,PRH_CST_UP,prh_rcp_type from "DEV_RAW"."EXCEL_QADDB"."VW_PRH_HIST"
qualify row_number() over (PARTITION BY upper(trim(PRH_NBR)),upper(trim(PRH_LINE)) order by PRH_RECEIVER,LOADDTS desc)=1) as PRH_HIST
on PRH_HIST.PRH_NBR = POD_DET.POD_NBR AND PRH_HIST.PRH_LINE = POD_DET.POD_LINE AND PRH_DOMAIN = 'EXCEL'
LEFT JOIN "DEV_RAW"."EXCEL_QADDB"."VW_IH_HIST" IH_HIST ON  IH_HIST.IH_NBR =TR_HIST.TR_NBR AND TR_TYPE in ('iss-so','ord-so') and IH_HIST.IH_DOMAIN = 'EXCEL'--40227426
LEFT JOIN "DEV_RAW"."EXCEL_QADDB"."VW_IDH_HIST" IDH_HIST ON IDH_HIST.IDH_NBR =TR_HIST.TR_NBR AND IDH_HIST.IDH_LINE = TR_HIST.TR_LINE and TR_TYPE in ('iss-so','ord-so')  AND IDH_HIST.IDH_DOMAIN ='EXCEL'--40227426
LEFT JOIN "DEV_RAW"."EXCEL_QADDB"."VW_IN_MSTR" IN_MSTR ON IN_MSTR.IN_SITE = TR_HIST.TR_SITE AND IN_MSTR.IN_PART = TR_HIST.TR_PART --AND IN_MSTR = 'EXCEL'--40227426
LEFT JOIN "DEV_RAW"."EXCEL_QADDB"."VW_SCT_DET" SCT_DET ON SCT_DET.SCT_SITE = IN_MSTR.IN_SITE AND SCT_DET.SCT_PART = IN_MSTR.IN_PART AND SCT_DOMAIN = 'EXCEL' AND UPPER(SCT_SIM) = 'current'
LEFT JOIN "DEV_RAW"."EXCEL_QADDB"."VW_LD_DET" LD_DET  ON LD_DET.LD_SITE = TR_HIST.TR_SITE AND LD_DET.LD_LOC = TR_HIST.TR_LOC AND LD_DET.LD_PART=TR_PART AND LD_DET.LD_LOT = TR_HIST.TR_SERIAL AND  LD_DOMAIN = 'EXCEL'--40227426
LEFT JOIN "DEV_RAW"."EXCEL_QADDB"."VW_LAD_DET" LAD_DET ON LAD_SITE =LD_SITE AND LAD_LOC = LD_LOC AND LAD_PART= LD_PART AND LAD_LOT = LD_LOT AND LAD_REF = LD_REF and LAD_DOMAIN ='EXCEL'--40227426
LEFT JOIN "DEV_RAW"."EXCEL_QADDB"."VW_SI_MSTR" SI_MSTR ON SI_MSTR.SI_SITE = TR_HIST.TR_SITE  and SI_DOMAIN ='EXCEL'--40227426
LEFT JOIN "DEV_RAW"."EXCEL_QADDB"."VW_LOC_MSTR" LOC_MSTR ON LOC_MSTR.LOC_SITE = SI_MSTR.SI_SITE AND LOC_DOMAIN = 'EXCEL'--40227426
LEFT JOIN "DEV_RAW"."EXCEL_QADDB"."VW_WO_MSTR" WO_MSTR ON WO_MSTR.WO_NBR = TR_HIST.TR_NBR AND WO_MSTR.WO_LOT=TR_HIST.TR_LOT AND TR_TYPE IN('rjct-wo','rct-wo') --40227426 
LEFT JOIN "DEV_RAW"."EXCEL_QADDB"."VW_TX2D_DET" TX2D_DET ON TX2D_DET.TX2D_REF = TR_HIST.TR_NBR AND TX2D_DET.TX2D_LINE = TR_HIST.TR_LINE and TX2D_DOMAIN = 'EXCEL' and TX2D_TR_TYPE in ('12','21')
LEFT JOIN "DEV_RAW"."EXCEL_QADDB"."VW_TX2_MSTR" TX2_MSTR ON TX2_MSTR.TX2_TAX_CODE = TX2D_DET.TX2D_TAX_CODE and TX2_DOMAIN = 'EXCEL'
LEFT JOIN "DEV_RAW"."EXCEL_QADDB"."VW_DSR_MSTR" DSR_MSTR ON DSR_MSTR.DSR_PART = PTP_DET.PTP_PART AND DSR_MSTR.DSR_SITE = PTP_DET.PTP_SITE AND DSR_DOMAIN = 'EXCEL'      
LEFT JOIN "DEV_RAW"."EXCEL_QADDB"."VW_DS_DET" DS_DET ON DS_DET.DS_PART = PTP_DET.PTP_PART AND DS_DET.DS_SITE = PTP_DET.PTP_SITE AND DS_DOMAIN = 'EXCEL'
LEFT JOIN "DEV_RAW"."EXCEL_QADDB"."VW_VD_MSTR" VD_MSTR ON VD_MSTR.VD_ADDR = PO_MSTR.PO_VEND AND UPPER(VD_DOMAIN) = 'EXCEL'
  