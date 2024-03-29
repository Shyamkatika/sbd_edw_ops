WITH TR_DIST as
(
SELECT DISTINCT TR_SITE,TR_PART,TR_DATE from "PROD_RAW"."EXCEL_QADDB"."VW_TR_HIST" WHERE  UPPER(TRIM(TR_DOMAIN)) = 'EXCEL' ),
TR_RANK AS
(
SELECT TR_SITE,TR_PART, TR_DATE, RANK() OVER (PARTITION BY  TR_SITE, TR_PART ORDER BY TR_SITE, TR_PART,TR_DATE DESC) as TR_RANK1
FROM TR_DIST 
) ,
SCT_DET AS
(
SELECT SCT_SITE,SCT_PART,SCT_CST_DATE,SCT_CST_TOT  
FROM "PROD_RAW"."EXCEL_QADDB"."VW_SCT_DET" WHERE  UPPER(TRIM(SCT_SIM)) = 'CURRENT' and SCT_PART='603137' and SCT_SITE='3'
)
SELECT  TR_SITE,TR_PART,TR_RANK1,TR_DATE ,  SCT_SITE,SCT_PART,SCT_CST_DATE,SCT_CST_TOT   FROM TR_RANK 
LEFT JOIN  SCT_DET 
on TR_SITE=SCT_SITE and SCT_PART=TR_PART
WHERE TR_PART='603137' and TR_SITE='3' and TR_RANK1 = 1 order by 1,2,3, 4 desc, 8 desc;



SELECT 
UPPER(concat(COALESCE(trim(tr_trnbr)::VARCHAR,''),'~',COALESCE(trim(YEAR(tr_date))::VARCHAR,''),'~',COALESCE(trim(tr_line)::VARCHAR,''))) AS INVTY_MVMNT_LN_KEY,
'EXCEL_QADDB' AS SRC_SYS_KEY,
tr_date AS SRC_RCRD_CREATE_DTE,
tr_userid AS SRC_RCRD_CREATE_USERID,
tr_date AS SRC_RCRD_UPD_DTE,
tr_userid AS SRC_RCRD_UPD_USERID, 
md5(UPPER(concat(COALESCE(trim(tr_trnbr)::VARCHAR,''),'~',COALESCE(trim(tr_date)::VARCHAR,''),'~',COALESCE(trim(tr_line)::VARCHAR,'')))) AS RCRD_HASH_KEY,
'N' AS  DEL_FROM_SRC_FLAG,  
'request' AS  ETL_INS_PID,                                                                  
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  ETL_INS_DTE,
'request' AS  ETL_UPD_PID,                                                                  
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  ETL_UPD_DTE,
TR_HIST.LOADDTS AS EFF_DTE,
'9999-12-31 00:00:00.000' AS EXPR_DTE, 
'Y' AS  CURR_RCRD_FLAG,                                                                                    
'N' AS  ORP_RCRD_FLAG,
TR_HIST.LOADDTS AS ZONE3_LOD_DTE,
TR_HIST.TR_LINE	AS	PROD_ORD_LN_NBR,
TR_HIST.TR_GL_AMT	AS	ACCT_VAL_LKEY,
TR_HIST.TR_UM	AS	PUR_DLV_QTY_UOM_KEY,
TR_HIST.TR_TYPE	AS	VAL_TEXT,
TR_HIST.TR_LINE	AS	ACCT_DOC_LN_1_NBR,
TR_HIST.TR_LINE	AS	ACCT_DOC_LN_2_NBR,
TR_HIST.TR_TYPE	AS	STK_MVMNT_TYP_LKEY,
ds_det.ds_order_category AS WHSE_MVMNT_TYP_LKEY,
TR_HIST.TR_BATCH    AS  BATCH_NBR_LKEY,
  CASE WHEN UPPER(TRIM(TR_HIST.TR_REF)) = 'BIN' THEN 'Y' 
  ELSE 'N'  END AS DYN_STORG_BIN_FLAG,
  CASE WHEN UPPER(TRIM(prh_hist.prh_cst_up)) = 'TRUE' THEN 'Y' 
     WHEN UPPER(TRIM(prh_hist.prh_cst_up)) = 'FALSE' THEN 'N' 
	 WHEN UPPER(TRIM(prh_hist.prh_cst_up)) is null THEN 'U'
	 ELSE prh_hist.prh_cst_up END AS GOODS_RCPT_VALUATED_FLAG,
  CASE WHEN UPPER(TRIM(prh_hist.prh_rcp_type)) = 'TRUE' THEN 'Y' 
     WHEN UPPER(TRIM(prh_hist.prh_rcp_type)) = 'FALSE' THEN 'N' 
	 WHEN UPPER(TRIM(prh_hist.prh_rcp_type)) is null THEN 'U'
	 ELSE prh_hist.prh_rcp_type END AS GOOD_RCPT_REVERSAL_FLAG,	 
--dsr_mstr.dsr_part AS TRNSFR_REQ_PRYRT_NBR,
--dsr_mstr.dsr_site AS WHSE_NBR,
--dsr_mstr.dsr_site as WHSE_LKEY,
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
--dsr_mstr.dsr_req_nbr AS TRNSFR_REQ_NBR,
--CONCAT(dsr_mstr.dsr_req_nbr,dsr_mstr.dsr_part) AS TRNSFR_REQ_SEQ_NBR,
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
--ih_hist.ih_cust	AS SENDER_CUST_NAME,
ad_mstr.ad_sort AS SENDER_CUST_NAME,
--lad_det.lad_lot AS LOT_NBR,
TR_HIST.TR_LOT AS LOT_NBR,
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
case when pt_mstr.pt_shelflife = '0' then null else pt_shelflife::timestamp_ntz end as SHELF_LIFE_EXPR_DTE,
--if pt_shelflife = 0 then null else pt_shelflife as SHELF_LIFE_EXPR_DTE
TR_HIST.TR_DATE as PROD_MVMNT_DTE,
TR_HIST.TR_PER_DATE AS  SVC_PERFORMER,
extract(year from tr_hist.tr_date) AS  PROD_MVMNT_DOC_YR,
CASE WHEN TR_HIST.TR_TYPE = 'CUM-RRES' THEN TR_HIST.tr_qty_chg ELSE 0 END AS CUML_RCV_QTY,
TR_HIST.tr_time::timestamp_ntz as SVCS_RENDERED_DTE,
VD_MSTR.VD_SORT AS SENDER_VEND_NAME,
tr_hist.tr_qty_chg AS PARALLEL_UOM_QTY,
VD_MSTR.VD_ADDR AS VEND_TO_ID,
CASE WHEN tr_type = 'ISS-SO' THEN TR_HIST.TR_LINE ELSE 0 END  AS  SLS_ORD_LN_NBR
FROM  {{source("EXCEL_QADDB","TR_HIST")}} TR_HIST
LEFT JOIN {{source("EXCEL_QADDB","AD_MSTR")}} AD_MSTR ON AD_MSTR.AD_ADDR =  TR_HIST.TR_ADDR  AND UPPER(TRIM(AD_DOMAIN)) = 'EXCEL'
LEFT JOIN {{source("EXCEL_QADDB","AR_MSTR")}} AR_MSTR ON AR_MSTR.AR_SO_NBR = TR_HIST.TR_NBR  AND AR_MSTR.AR_NBR  = TR_HIST.TR_RMKS AND UPPER(TRIM(AR_MSTR.AR_DOMAIN)) = 'EXCEL' AND AR_MSTR.AR_TYPE = 'I'
LEFT JOIN {{source("EXCEL_QADDB","PT_MSTR")}} PT_MSTR ON PT_MSTR.PT_PART = TR_HIST.TR_PART AND UPPER(TRIM(PT_MSTR.PT_DOMAIN)) = 'EXCEL'
LEFT JOIN {{source("EXCEL_QADDB","PL_MSTR")}} PL_MSTR ON PL_MSTR.PL_PROD_LINE = PT_MSTR.PT_PROD_LINE AND UPPER(TRIM(PL_DOMAIN)) = 'EXCEL'
LEFT JOIN {{source("EXCEL_QADDB","PTP_DET")}} PTP_DET ON PTP_DET.PTP_PART = TR_HIST.TR_PART AND PTP_DET.PTP_SITE = TR_HIST.TR_SITE AND UPPER(TRIM(PTP_DOMAIN)) = 'EXCEL'
LEFT JOIN (Select * from {{source("EXCEL_QADDB","PS_MSTR")}} QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(ps_comp) ORDER BY PS_PAR,PS_START,PS_MOD_DATE ) =1 ) ps_mstr ON UPPER(PT_MSTR.PT_PART) = UPPER(ps_mstr.ps_comp) AND UPPER(TRIM(PS_DOMAIN)) = 'EXCEL'
LEFT JOIN {{source("EXCEL_QADDB","POD_DET")}} POD_DET ON POD_DET.POD_NBR = TR_HIST.TR_NBR AND POD_DET.POD_LINE = TR_HIST.TR_LINE and TR_TYPE in('RCT-PO','ISS-PRV') AND UPPER(TRIM(POD_DOMAIN)) = 'EXCEL'
LEFT JOIN {{source("EXCEL_QADDB","PO_MSTR")}} PO_MSTR ON PO_MSTR.PO_NBR = TR_HIST.TR_NBR AND UPPER(TRIM(PO_DOMAIN))= 'EXCEL'
LEFT JOIN {{source("EXCEL_QADDB", "PRH_HIST")}} PRH_HIST ON PRH_HIST.PRH_NBR = TR_HIST.TR_NBR AND PRH_HIST.PRH_LINE = TR_HIST.TR_LINE AND PRH_HIST.PRH_PART = TR_HIST.TR_PART AND PRH_HIST.PRH_RECEIVER = TR_HIST.TR_LOT AND PRH_HIST.PRH_RCP_DATE = TR_HIST.TR_DATE AND UPPER(TRIM(PRH_DOMAIN)) = 'EXCEL'
--LEFT JOIN (select prh_receiver,PRH_NBR,PRH_LINE,PRH_PS_QTY,PRH_RCVD,PRH_PS_NBR,PRH_VEND,PRH_DOMAIN,PRH_CST_UP,prh_rcp_type from {{source("EXCEL_QADDB", "PRH_HIST")}} qualify row_number() over (PARTITION BY upper(trim(PRH_NBR)),upper(trim(PRH_LINE)) order by PRH_RECEIVER,LOADDTS desc)=1) as PRH_HIST on PRH_HIST.PRH_NBR = POD_DET.POD_NBR AND PRH_HIST.PRH_LINE = POD_DET.POD_LINE AND UPPER(TRIM(PRH_DOMAIN)) = 'EXCEL'
LEFT JOIN {{source("EXCEL_QADDB","IH_HIST")}} IH_HIST ON IH_HIST.IH_NBR = TR_HIST.TR_NBR AND TR_TYPE in ('iss-so','ord-so') and UPPER(TRIM(IH_HIST.IH_DOMAIN)) = 'EXCEL'
--LEFT JOIN {{source("EXCEL_QADDB","IDH_HIST")}} IDH_HIST ON IDH_HIST.IDH_NBR =TR_HIST.TR_NBR  AND IDH_HIST.IDH_LINE = TR_HIST.TR_LINE and TR_TYPE in ('iss-so','ord-so')  AND UPPER(TRIM(IDH_HIST.IDH_DOMAIN)) ='EXCEL'
LEFT JOIN {{source("EXCEL_QADDB","IDH_HIST")}}  IDH_HIST  ON 
IDH_HIST.IDH_NBR =TR_HIST.TR_NBR  AND 
IDH_HIST.IDH_LINE = TR_HIST.TR_LINE AND 
IDH_HIST.IDH_PART = TR_HIST.TR_SITE and UPPER(TR_TYPE) in ('ISS-SO') AND UPPER(TRIM(IDH_HIST.IDH_DOMAIN)) ='EXCEL'
LEFT JOIN {{source("EXCEL_QADDB","IN_MSTR")}} IN_MSTR ON IN_MSTR.IN_SITE = TR_HIST.TR_SITE AND IN_MSTR.IN_PART = TR_HIST.TR_PART AND UPPER(TRIM(IN_DOMAIN)) ='EXCEL'
--LEFT JOIN {{source("EXCEL_QADDB","SCT_DET")}} SCT_DET ON SCT_DET.SCT_SITE = IN_MSTR.IN_SITE AND SCT_DET.SCT_PART = IN_MSTR.IN_PART AND UPPER(TRIM(SCT_DOMAIN)) = 'EXCEL' AND UPPER(TRIM(SCT_SIM)) = 'CURRENT'

LEFT JOIN {{source("EXCEL_QADDB","SCT_DET")}} SCT_DET ON SCT_DET.SCT_SITE = TR_HIST.TR_SITE 
AND SCT_DET.SCT_PART = TR_HIST.TR_PART AND 
SCT_DET.SCT_CST_DATE >= TR_HIST.TR_DATE AND UPPER(TRIM(SCT_DOMAIN)) = 'EXCEL' AND UPPER(TRIM(SCT_SIM)) = 'CURRENT'


LEFT JOIN {{source("EXCEL_QADDB","LD_DET")}} LD_DET  ON UPPER(TRIM(LD_DET.LD_SITE)) = UPPER(TRIM(TR_HIST.TR_SITE)) AND UPPER(TRIM(LD_DET.LD_LOC)) = UPPER(TRIM(TR_HIST.TR_LOC)) AND UPPER(TRIM(LD_DET.LD_PART)) = UPPER(TRIM(TR_HIST.TR_PART)) AND UPPER(TRIM(LD_DET.LD_LOT)) = UPPER(TRIM(TR_HIST.TR_SERIAL)) AND UPPER(TRIM(LD_DET.LD_REF)) = UPPER(TRIM(TR_HIST.TR_REF)) AND UPPER(TRIM(LD_DOMAIN)) = 'EXCEL'
LEFT JOIN (SELECT LAD_SITE,LAD_PART,LAD_DOMAIN,LAD_LOT FROM {{source("EXCEL_QADDB", "LAD_DET")}} QUALIFY ROW_NUMBER() OVER (PARTITION BY LAD_SITE,LAD_PART ORDER BY PRO2MODIFIED desc) =1) LAD_DET ON UPPER(TRIM(LAD_SITE)) = UPPER(TRIM(LD_SITE)) AND UPPER(TRIM(LAD_PART)) = UPPER(TRIM(LD_PART)) AND UPPER(TRIM(LAD_DOMAIN)) = 'EXCEL'
LEFT JOIN {{source("EXCEL_QADDB","SI_MSTR")}} SI_MSTR ON SI_MSTR.SI_SITE = TR_HIST.TR_SITE and UPPER(TRIM(SI_DOMAIN)) ='EXCEL'
LEFT JOIN (SELECT * FROM {{source("EXCEL_QADDB","LOC_MSTR")}} LOC_MSTR QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(LOC_MSTR.LOC_SITE)),UPPER(TRIM(LOC_MSTR.LOC_DOMAIN)) ORDER BY LOC_LOC,LOC_SITE,LOC_DOMAIN ) =1) LOC_MSTR ON UPPER(TRIM(LOC_MSTR.LOC_SITE))=UPPER(TRIM(SI_MSTR.SI_SITE)) AND UPPER(TRIM(LOC_MSTR.LOC_DOMAIN)) = 'EXCEL'
LEFT JOIN {{source("EXCEL_QADDB","WO_MSTR")}} WO_MSTR ON 
UPPER(TRIM(WO_MSTR.WO_NBR)) = UPPER(TRIM(TR_HIST.TR_NBR)) AND 
UPPER(TRIM(WO_MSTR.WO_LOT)) = UPPER(TRIM(TR_HIST.TR_LOT)) AND 
UPPER(TRIM(WO_MSTR.WO_PART)) = UPPER(TRIM(TR_HIST.TR_PART)) AND 
UPPER(TRIM(TR_HIST.TR_TYPE)) IN ('RJCT-WO','RCT-WO') AND UPPER(TRIM(WO_DOMAIN)) = 'EXCEL' 
LEFT JOIN {{source("EXCEL_QADDB","TX2D_DET")}} TX2D_DET 
ON TX2D_DET.TX2D_REF = TR_HIST.TR_NBR 
AND TX2D_DET.TX2D_LINE = TR_HIST.TR_LINE 
AND UPPER(TRIM(TX2D_DET.TX2D_REF)) = UPPER(TRIM(TR_HIST.TR_REF))
and UPPER(TRIM(TX2D_DOMAIN)) = 'EXCEL' --and TX2D_TR_TYPE in ('12','21')
LEFT JOIN {{source("EXCEL_QADDB","TX2_MSTR")}} TX2_MSTR ON 
TX2_MSTR.TX2_TAX_CODE = TX2D_DET.TX2D_TAX_CODE AND 
TX2_MSTR.TX2_TAX_TYPE = TX2D_DET.TX2D_TR_TYPE AND UPPER(TRIM(TX2_DOMAIN)) = 'EXCEL'
--LEFT JOIN (SELECT * FROM {{source("EXCEL_QADDB","DSR_MSTR")}} QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(DSR_PART),UPPER(DSR_SITE) ORDER BY LOADDTS) =1) DSR_MSTR ON UPPER(TRIM(DSR_MSTR.DSR_PART)) = UPPER(TRIM(PTP_DET.PTP_PART)) AND UPPER(TRIM(DSR_MSTR.DSR_SITE)) = UPPER(TRIM(PTP_DET.PTP_SITE)) AND UPPER(TRIM(DSR_DOMAIN)) = 'EXCEL'
--1LEFT JOIN "DEV_RAW"."EXCEL_QADDB"."DSR_MSTR" DSR_MSTR ON UPPER(TRIM(DSR_MSTR.DSR_PART)) = UPPER(TRIM(TR_HIST.TR_PART)) AND UPPER(TRIM(DSR_MSTR.DSR_SITE)) = UPPER(TRIM(TR_HIST.TR_SITE)) AND UPPER(TRIM(DSR_DOMAIN)) = 'EXCEL' 
LEFT JOIN  {{source("EXCEL_QADDB","DS_DET")}} DS_DET
on UPPER(TRIM(DS_DET.DS_NBR)) = UPPER(TRIM(TR_HIST.TR_NBR))
AND UPPER(TRIM(DS_DET.DS_SITE)) = UPPER(TRIM(TR_HIST.TR_REF_SITE))
AND UPPER(TRIM(DS_DET.DS_PART)) = UPPER(TRIM(TR_HIST.TR_PART))
AND UPPER(TRIM(DS_DET.DS_LINE)) = UPPER(TRIM(TR_HIST.TR_LINE))
AND UPPER(TRIM(DS_DET.DS_LOC)) = UPPER(TRIM(TR_hIST.TR_LOC))
AND UPPER(TRIM(DS_DET.DS_SHIPDATE)) = UPPER(TRIM(TR_HIST.TR_DATE))
AND UPPER(TRIM(DS_DOMAIN)) = 'EXCEL'
LEFT JOIN {{source("EXCEL_QADDB","VD_MSTR")}} VD_MSTR ON VD_MSTR.VD_ADDR = PO_MSTR.PO_VEND AND UPPER(TRIM(VD_DOMAIN)) = 'EXCEL' 
WHERE UPPER(TRIM(TR_HIST.TR_DOMAIN)) = 'EXCEL'

