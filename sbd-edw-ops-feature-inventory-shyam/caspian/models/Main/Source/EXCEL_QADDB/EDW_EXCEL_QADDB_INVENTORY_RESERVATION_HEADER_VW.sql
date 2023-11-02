SELECT 
UPPER(concat(COALESCE(trim(WO_MSTR.WO_NBR)::VARCHAR,''),'~',
             COALESCE(trim(WO_MSTR.WO_LOT)::VARCHAR,''))) as INVTY_RSRV_HDR_KEY,
'EXCEL_QADDB' AS SRC_SYS_KEY,
wo_mstr.wo_ord_date AS {{var('column_SRC_RCRD_CREATE_DTE')}},
NULL AS {{var('column_SRC_RCRD_CREATE_USERID')}},
NULL AS {{var('column_SRC_RCRD_UPD_DTE')}},
NULL AS {{var('column_SRC_RCRD_UPD_USERID')}}, 
md5(UPPER(concat(COALESCE(trim(WO_MSTR.WO_NBR)::VARCHAR,''),'~',COALESCE(trim(WO_MSTR.WO_LOT)::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS  {{var('column_DEL_FROM_SRC_FLAG')}},  
'{{model.name}}' AS  {{var('column_ETL_INS_PID')}},  
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS  {{var('column_ETL_UPD_PID')}},  
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_UPD_DTE')}},
wo_mstr.LOADDTS AS {{var('column_vereffdte')}}, 
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}}, 
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
wo_mstr.LOADDTS AS {{var('column_z3loddtm')}},
--wo_mstr.wo_expire as EXPR_DTE, 
wo_mstr.wo_record_date as RPT_ENTR_NBR,
wo_mstr.wo_vend as SENDER_VEND_NAME, 
wo_mstr.wo_nbr as ORD_NBR, 
wo_mstr.wo_routing as OPER_RTG_NBR, 
concat(wo_mstr.wo_acct,wo_mstr.wo_cc) as FUND_CNTR_ORGANIZATIONAL_NAME, 
wo_mstr.wo_acct as FUNC_AREA, 
wo_mstr.wo_cc as FUNDS_CNTR, 
wo_mstr.wo_xvar_cc as COST_ACCTG_CD, 
wo_mstr.wo_bom_code as WORK_BRKDN_STRC_TEXT, 
wo_mstr.wo_rel_date as RSRV_BASE_DTE, 
wo_mstr.wo_loc as RCV_ISS_SLOC_KEY, 
wo_mstr.wo_iss_site as RCV_ISS_PLANT_KEY, 
wo_mstr.wo_per_date as REF_SETLMNT_DTE, 
wo_mstr.wo_joint_type as BUS_PROC, 
wo_mstr.wo_batch as JRNL_BATCH_NAME, 
wo_mstr.wo_line as PRODTN_ORD_LN_NBR, 
wo_mstr.wo_itm_line as REF_LN_NBR, 
wo_mstr.wo_itm_line as PROD_LEDGER_ENTR_NBR, 
wo_mstr.wo_due_date as EXPCT_RCPT_DTE, 
wo_mstr.wo_expire as NEW_EXPR_DTE, 
wo_mstr.wo_nbr as RSRV_ENTR_NBR, 
wo_mstr.wo_so_job as DISALLOW_RSRV_CNCL_FLAG, 
wo_mstr.wo_status as PROD_RSRV_STAT_TYP, 
wo_mstr.wo_itm_line as LN_NBR, 
wo_mstr.wo_qty_ord as PROD_RESERVED_QTY, 
-- wo_mstr.wo_expire as LOT_DOMAIN_CDEXPR_DTE, 
wo_mstr.wo_loc as RSRV_LOC_LKEY, 
wo_mstr.wo_cc as PROFIT_CNTR_NBR, 
wo_mstr.wo_nbr_lines as PROD_RSRV_NBR, 
wo_mstr.wo_qty_ord as RESERVED_PROD_QTY,
wo_mstr.wo_domain as DOMAIN_CD,
wo_mstr.wo_cc as CSTCTR_NAME, 
wo_mstr.wo_loc as PROD_SLOC_ID, 
wo_mstr.wo_serial as PROD_SERL_NBR,
case when tr_hist.TR_TYPE like '%WO' then TR_TYPE else null end as MVMNT_TYP_NBR,
--tr_hist.TR_TYPE as MVMNT_TYP_NBR,
CASE WHEN tr_type IN ('rct-po','iss-prv','iss-rv','ord-po') then tr_nbr else null END AS PO_NBR,  
tr_hist.tr_nbr as SLS_ORD_HDR_KEY, 
tr_hist.tr_addr as CUST_KEY,
wo_mstr.wo_bom_code as BOM_EXPLD_NBR,
ad_mstr.ad_addr as PTNR_ACCT_NBR, 
en_mstr.en_entity as CLEARING_CO_CD_LKEY, 
ad_mstr.ad_sort as CUST_NAME, 
CONCAT(pt_desc1,pt_desc2) as PROD_DESC,
case when mrp_detail = '' then 'N' else 'Y' end as SUPPLY_DMD_FLAG,
mrp_det.mrp_part as RESERVED_PROD_KEY, 
wo_mstr.wo_loc as LOC_KEY,
mrp_det.mrp_type as REL_SRC_TYP_LKEY, 
mrp_det.mrp_detail as REL_SRC_STYP_LKEY, 
tr_hist.tr_ship_date as EXPCT_SHIP_DTE,
tr_hist.tr_um as UOM_KEY, 
--wod_det.wod_mvuse_post as PRTL_QTY, 
--wod_det.wod_mvuse_post as PRTL_INVC_QTY, 
--sod_det.sod_part as DLV_SCHED_HDR_NBR, 
--pod_det.pod_part as PUR_PROD_NBR,
--sod_det.sod_nbr as PROD_NBR, 
WO_MSTR.WO_PART AS PROD_NBR,
tr_hist.tr_enduser as SHIPTO_CUST_ID,
si_mstr.si_entity as TRDG_PTNR_CO_ID, 
case when cm_mstr.cm_type = '' THEN 'Y' ELSE 'N' END as CUST_GRP_FLAG, 
en_mstr.en_entity as CO_HOLDING,
ad_mstr.ad_country as ENTY_CNTRY,
ad_mstr.ad_sort as SENDER_CUST_NAME,
INVC_QTY as INVC_QTY
FROM {{source("EXCEL_QADDB", "WO_MSTR")}}  AS  WO_MSTR --354723
LEFT JOIN {{source("EXCEL_QADDB", "MRP_DET")}}  MRP_DET ON UPPER(TRIM(MRP_DET.mrp_part))= UPPER(TRIM(WO_MSTR.wo_part)) and UPPER(TRIM(MRP_DET.mrp_nbr)) = UPPER(TRIM(WO_MSTR.wo_nbr)) and UPPER(TRIM(mrp_line)) = UPPER(TRIM(wo_lot)) AND UPPER(TRIM(mrp_domain)) ='EXCEL' and UPPER(TRIM(mrp_dataset)) = 'WO_MSTR'--354723
LEFT JOIN {{source("EXCEL_QADDB", "PT_MSTR")}}  PT_MSTR ON UPPER(TRIM(PT_MSTR.pt_part)) = UPPER(TRIM(WO_MSTR.wo_part)) AND UPPER(TRIM(pt_domain)) = 'EXCEL' 
LEFT JOIN (SELECT * FROM {{source("EXCEL_QADDB", "TR_HIST")}} WHERE UPPER(TRIM(TR_DOMAIN)) = 'EXCEL'
QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(TRIM(TR_NBR)),UPPER(TRIM(TR_PART)),UPPER(TRIM(TR_LOT)) ORDER BY PRO2CREATED DESC)=1) TR_HIST ON 
UPPER(TRIM(TR_HIST.tr_nbr)) = UPPER(TRIM(WO_MSTR.wo_nbr)) and 
UPPER(TRIM(TR_HIST.tr_part)) = UPPER(TRIM(WO_MSTR.wo_part)) and 
UPPER(TRIM(TR_HIST.tr_lot)) = UPPER(TRIM(WO_MSTR.wo_lot)) AND 
UPPER(TRIM(tr_domain)) = 'EXCEL'
--LEFT JOIN {{source("EXCEL_QADDB", "TR_HIST")}}  TR_HIST ON UPPER(TRIM(TR_HIST.tr_nbr)) = UPPER(TRIM(WO_MSTR.wo_nbr)) and UPPER(TRIM(TR_HIST.tr_part)) = UPPER(TRIM(WO_MSTR.wo_part)) and UPPER(TRIM(TR_HIST.tr_lot)) = UPPER(TRIM(WO_MSTR.wo_lot)) AND UPPER(TRIM(tr_domain)) = 'EXCEL'
LEFT JOIN {{source("EXCEL_QADDB", "SI_MSTR")}}  SI_MSTR ON UPPER(TRIM(SI_MSTR.si_site)) =  UPPER(TRIM(WO_MSTR.wo_site)) AND UPPER(TRIM(si_domain)) = 'EXCEL'
LEFT JOIN {{source("EXCEL_QADDB", "EN_MSTR")}}  EN_MSTR ON UPPER(TRIM(EN_MSTR.en_entity)) = UPPER(TRIM(SI_MSTR.si_entity)) AND  UPPER(TRIM(en_domain)) = 'EXCEL'
LEFT JOIN {{source("EXCEL_QADDB", "AD_MSTR")}}  AD_MSTR ON UPPER(TRIM(AD_MSTR.AD_ADDR)) = UPPER(TRIM(WO_SITE)) AND UPPER(TRIM(AD_DOMAIN)) = 'EXCEL'
LEFT JOIN {{source("EXCEL_QADDB", "CM_MSTR")}}  CM_MSTR ON UPPER(TRIM(CM_MSTR.CM_ADDR)) = UPPER(TRIM(wo_mstr.wo_site)) AND UPPER(TRIM(CM_DOMAIN)) = 'EXCEL'
left join (select sum(wo_qty_chg) INVC_QTY,wo_serial from {{source("EXCEL_QADDB", "WO_MSTR")}} 
group by wo_serial) as wo on wo.wo_serial = WO_MSTR.wo_serial

--LEFT JOIN {{source("EXCEL_QADDB", "POD_DET")}}  POD_DET ON  UPPER(POD_DET.POD_PART)=UPPER(MRP_DET.MRP_PART) and UPPER(POD_DET.POD_PART)=UPPER(MRP_DET.MRP_NBR) and POD_DET.POD_LINE=MRP_DET.MRP_LINE
--LEFT JOIN {{source("EXCEL_QADDB", "WOD_DET")}}  WOD_DET ON UPPER(TRIM(WOD_DET.wod_lot)) = UPPER(TRIM(WO_MSTR.wo_lot)) and UPPER(TRIM(WOD_DET.WOD_ITM_LINE)) = UPPER(TRIM(WO_MSTR.wo_itm_line)) AND UPPER(TRIM(WOD_DET.WOD_PART)) = UPPER(TRIM(WO_MSTR.WO_PART)) AND UPPER(TRIM(wod_domain)) = 'EXCEL' --354723
--LEFT JOIN {{source("EXCEL_QADDB", "SOD_DET")}}  SOD_DET ON  UPPER(TRIM(SOD_DET.SOD_NBR)) = UPPER(TRIM(WOD_DET.WOD_SOD_NBR)) AND UPPER(TRIM(SOD_DET.SOD_LINE)) = UPPER(TRIM(WOD_DET.WOD_SOD_LINE)) AND UPPER(TRIM(SOD_DOMAIN)) = 'EXCEL'
--LEFT JOIN {{source("EXCEL_QADDB", "SO_MSTR")}}  SO_MSTR on SOD_DET.SOD_NBR = SO_MSTR.SO_NBR
--LEFT JOIN {{source("EXCEL_QADDB", "AD_MSTR")}}  AD_MSTR ON UPPER(TRIM(AD_MSTR.AD_ADDR)) = UPPER(TRIM(SO_MSTR.SO_CUST))
--LEFT JOIN {{source("EXCEL_QADDB", "PT_MSTR")}}  PT_MSTR ON  UPPER(TRIM(PT_MSTR.pt_part)) = UPPER(TRIM(WO_MSTR.wo_part)) AND UPPER(TRIM(pt_domain)) = 'EXCEL' --AND UPPER(TRIM(wo_status)) IN  ('F','E')--354723


