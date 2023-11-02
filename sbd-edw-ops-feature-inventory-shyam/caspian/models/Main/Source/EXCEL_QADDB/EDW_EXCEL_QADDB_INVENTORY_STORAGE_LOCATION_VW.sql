
SELECT 
UPPER(concat(COALESCE(trim(LD_LOC)::VARCHAR,''),'~',
             COALESCE(trim(LD_LOT)::VARCHAR,''),'~',
             COALESCE(trim(LD_PART)::VARCHAR,''),'~',
             COALESCE(trim(LD_REF)::VARCHAR,''),'~',
             COALESCE(trim(LD_SITE)::VARCHAR,''))) AS INVTY_SLOC_KEY,
'EXCEL_QADDB' AS SRC_SYS_KEY,
ld_det.ld_date AS SRC_RCRD_CREATE_DTE, 
null AS SRC_RCRD_CREATE_USERID,
null AS SRC_RCRD_UPD_DTE,
null AS SRC_RCRD_UPD_USERID,
md5(UPPER(concat(COALESCE(trim(LD_LOC)::VARCHAR,''),'~',
             COALESCE(trim(LD_LOT)::VARCHAR,''),'~',
             COALESCE(trim(LD_PART)::VARCHAR,''),'~',
             COALESCE(trim(LD_REF)::VARCHAR,''),'~',
             COALESCE(trim(LD_SITE)::VARCHAR,'')))) AS RCRD_HASH_KEY,
'N' AS  DEL_FROM_SRC_FLAG,  
'EDW_EXCEL_QADDB_INVENTORY_STORAGE_LOCATION_VW' AS  ETL_INS_PID,                                                                  
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  ETL_INS_DTE,
'EDW_EXCEL_QADDB_INVENTORY_STORAGE_LOCATION_VW' AS  ETL_UPD_PID,                                                                  
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  ETL_UPD_DTE,
LD_DET.LOADDTS AS EFF_DTE,
'9999-12-31 00:00:00.000' AS EXPR_DTE,
'Y' AS  CURR_RCRD_FLAG,                                                                                    
'N' AS  ORP_RCRD_FLAG,
LD_DET.LOADDTS AS ZONE3_LOD_DTE,
--ld_det.ld_date	AS	SRC_RCRD_CREATE_DTE,
--loc_mstr.loc_date AS EFF_DTE,
--ld_det.ld_expire AS EXPR_DTE,
ld_det.ld_loc	AS	MRP_SLOC_LKEY,
ad_mstr.ad_addr AS	CUST_KEY,
loc_mstr.loc_loc AS	SLOC_NAME,
in_mstr.in_loc	AS	PTNR_SLOC_KEY,
si_mstr.si_entity	AS	ORG_KEY,
loc_mstr.loc_site	AS	SHIP_LOC_KEY,
CASE WHEN UPPER(TRIM(pt_mstr.pt_spec_hdlg)) = 'TRUE' THEN 'Y'
     WHEN UPPER(TRIM(pt_mstr.pt_spec_hdlg)) = 'FALSE' THEN 'N'
     WHEN UPPER(TRIM(pt_mstr.pt_spec_hdlg)) is null THEN 'U'
     ELSE pt_mstr.pt_spec_hdlg END AS HDLNG_UNIT_REQ_FLAG, 
pt_mstr.pt_pick_logic	AS	NGTV_STK_QTY_ALLOW_FLAG,
--  CASE WHEN UPPER(TRIM(pt_mstr.pt_pick_logic)) = 1 THEN 'Y'
--       WHEN UPPER(TRIM(pt_mstr.pt_pick_logic)) = 0 THEN 'N'
--  	 ELSE pt_mstr.pt_pick_logic END AS NGTV_STK_QTY_ALLOW_FLAG,
ld_det.ld_ref	AS	PAL_NBR,
in_mstr.in_rec_date	AS	RCRD_PROC_DTE,
tr_hist.tr_nbr	AS	PROD_ORD_NBR,
prh_hist.prh_ps_nbr	AS	PKG_NBR,
ptp_det.ptp_vend AS SUPLR_NBR,
SI_MSTR.SI_DESC AS	WHSE_NAME,
AD_MSTR.AD_NAME AS CNTCT_PRSN_NAME,
loc_mstr.loc_desc AS WHSE_DESC_TEXT,
ad_mstr.ad_line1 AS	ADDR_LN_1_TEXT,
ad_mstr.ad_line2 AS	ADDR_LN_2_TEXT,
ad_mstr.ad_ctry	AS	CNTRY_KEY,
ad_mstr.ad_zip	AS	ZIP_NBR,
ld_det.ld_part	AS	PHN_NBR,
ld_det.ld_ref	AS	WHSE_PAL_CD_LKEY,
loc_mstr.loc_type AS WHSE_OPER_TYP_LKEY,
ad_mstr.ad_city	AS	CITY_NAME,
ad_mstr.ad_state AS	STATE_NAME,
ld_det.ld_site	AS	PLANT_LOC_NAME,
ad_mstr.ad_addr	AS	VEND_ID,
loc_mstr.loc_loc AS	SLOC_ID,
in_mstr.in_wh AS WMS_SCHEMA_NAME
--code_mstr.code_value AS CARRIER_CD_NBR 
--in_mstr.in_wh AS WMS_SCHEMA_NAME
from {{source('EXCEL_QADDB','LD_DET')}} LD_DET 
LEFT JOIN  {{source('EXCEL_QADDB','LOC_MSTR')}} LOC_MSTR ON UPPER (LOC_MSTR.LOC_SITE) = UPPER(LD_DET.LD_SITE) AND UPPER(LOC_MSTR.LOC_LOC) = UPPER(LD_DET.LD_LOC) AND UPPER(LOC_MSTR.LOC_DOMAIN) = 'EXCEL'
LEFT JOIN (Select tr_batch,tr_part,tr_site,TR_LOC,TR_NBR,TR_LINE,TR_TYPE,TR_ADDR,TR_SERIAL,TR_LOT from {{source('EXCEL_QADDB','TR_HIST')}} where upper(trim(tr_domain))='EXCEL'
QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(tr_part),UPPER(tr_site),UPPER(TR_LOC) ORDER BY TR_TRNBR) =1 ) tr_hist ON  UPPER(LD_DET.LD_PART ) = UPPER(tr_hist.tr_part) AND UPPER(LD_DET.LD_SITE) = UPPER(tr_hist.tr_site) and upper(LD_DET.LD_LOC) =  UPPER(TR_HIST.TR_LOC ) and UPPER(LD_DET.LD_LOT) =  UPPER(TR_HIST.TR_SERIAL) 
LEFT JOIN {{source('EXCEL_QADDB','PTP_DET')}} PTP_DET ON UPPER(PTP_DET.PTP_PART) = UPPER(TR_HIST.TR_PART) AND UPPER(PTP_DET.PTP_SITE) = UPPER(TR_HIST.TR_SITE) AND UPPER(PTP_DOMAIN) = 'EXCEL'
--LEFT JOIN {{source('EXCEL_QADDB','PRH_HIST')}} PRH_HIST ON UPPER(PRH_HIST.PRH_NBR) = UPPER(TR_HIST.TR_NBR) AND UPPER(PRH_HIST.PRH_LINE) =UPPER(TR_HIST.TR_LINE) AND UPPER(TR_TYPE) IN ('RCT-PO','ISS-PRV') AND UPPER(PRH_HIST.PRH_DOMAIN) = 'EXCEL'
LEFT JOIN {{source('EXCEL_QADDB','PRH_HIST')}} PRH_HIST ON UPPER(PRH_HIST.PRH_NBR) = UPPER(TR_HIST.TR_NBR) AND UPPER(PRH_HIST.PRH_LINE) =UPPER(TR_HIST.TR_LINE) AND UPPER(PRH_HIST.PRH_RECEIVER) = UPPER(TR_HIST.TR_LOT) and UPPER(TR_TYPE) IN ('RCT-PO','ISS-PRV') and UPPER(PRH_HIST.PRH_DOMAIN) = 'EXCEL'
LEFT JOIN {{source('EXCEL_QADDB','IN_MSTR')}} IN_MSTR ON UPPER(IN_MSTR.IN_SITE) = UPPER(LD_DET.LD_SITE) AND UPPER(IN_MSTR.IN_PART) = UPPER(LD_DET.LD_PART) AND UPPER(IN_DOMAIN) = 'EXCEL' 
LEFT JOIN {{source('EXCEL_QADDB','AD_MSTR')}} AD_MSTR ON UPPER(AD_MSTR.AD_ADDR) =  UPPER(TR_HIST.TR_ADDR) AND UPPER(AD_DOMAIN) = 'EXCEL'
LEFT JOIN {{source('EXCEL_QADDB','SI_MSTR')}} SI_MSTR ON UPPER(SI_MSTR.SI_SITE) = UPPER(LD_DET.LD_SITE) AND UPPER(SI_DOMAIN) = 'EXCEL' 
LEFT JOIN {{source('EXCEL_QADDB','PT_MSTR')}} PT_MSTR ON  UPPER(PT_MSTR.PT_PART) = UPPER(TR_HIST.TR_PART) AND UPPER(PT_MSTR.PT_DOMAIN) = 'EXCEL' 

