SELECT
 UPPER(CONCAT(coalesce(trnitem.trn_num::VARCHAR,''),'~',coalesce(trnitem.site_ref::VARCHAR,'')
,'~',coalesce(trnitem.TRN_LINE::VARCHAR,'')))	as	WHSE_TRNSFR_ORD_LN_KEY	,
'SLIES' AS {{var('column_srcsyskey')}},
MD5(UPPER(CONCAT(coalesce(trnitem.trn_num::VARCHAR,''),'~',coalesce(trnitem.site_ref::VARCHAR,'')
,'~',coalesce(trnitem.TRN_LINE::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
TRNITEM.LOADDTS AS {{var('column_z3loddtm')}},
TRNITEM.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
case when length(trim(UPPER((trnitem.site_ref)))) <1 then {{var('default_mapkey')}} when UPPER(trnitem.site_ref)
IS NULL then {{var('default_mapkey')}} else UPPER(trnitem.site_ref) end AS TXN_SITE_LKEY,


case when length(trim(UPPER((trnitem.trn_num)))) <1 then {{var('default_mapkey')}} when UPPER(trnitem.trn_num)
IS NULL then {{var('default_mapkey')}} else UPPER(trnitem.trn_num) end AS STK_TRNSFR_LOC_KEY,


case when length(trim(UPPER((trnitem.trn_line)))) <1 then {{var('default_mapkey')}} when UPPER(trnitem.trn_line)
IS NULL then {{var('default_mapkey')}} else UPPER(trnitem.trn_line) end AS PROD_TRNSFR_STAT_LKEY,


case when length(trim(UPPER((trnitem.item)))) <1 then {{var('default_mapkey')}} when UPPER(trnitem.item)
IS NULL then {{var('default_mapkey')}} else UPPER(trnitem.item) end AS PROD_GRP_LKEY,


case when length(trim(UPPER((trnitem.trn_loc)))) <1 then {{var('default_mapkey')}} when UPPER(trnitem.trn_loc)
IS NULL then {{var('default_mapkey')}} else UPPER(trnitem.trn_loc) end AS ORIG_TRNSFR_LOC_KEY,


case when length(trim(UPPER((trnitem.frm_ref_type)))) <1 then {{var('default_mapkey')}} when UPPER(trnitem.frm_ref_type)
IS NULL then {{var('default_mapkey')}} else UPPER(trnitem.frm_ref_type) end AS REF_DOC_TYP_LKEY,


case when length(trim(UPPER((trnitem.u_m)))) <1 then {{var('default_mapkey')}} when UPPER(trnitem.u_m)
IS NULL then {{var('default_mapkey')}} else UPPER(trnitem.u_m) end AS BASE_ON_HAND_BAL_UOM_KEY,

---trnitem.site_ref	as	TXN_SITE_LKEY	,
---trnitem.trn_num	as	STK_TRNSFR_LOC_KEY	,
trnitem.trn_line	as	TRNSFR_ORD_LN_NBR	,
--trnitem.stat	as	PROD_TRNSFR_STAT_LKEY	,
--trnitem.item	as	PROD_GRP_LKEY	,
---trnitem.trn_loc	as	ORIG_TRNSFR_LOC_KEY	,
trnitem.ship_date	as	ORD_SHIP_DTE	,
trnitem.rcvd_date	as	ORD_RCV_DTE	,
trnitem.qty_req	as	DEST_RQST_QTY	,
trnitem.qty_shipped	as	PROD_SHIP_QTY	,
trnitem.qty_received	as	PROD_RCV_QTY	,
trnitem.qty_loss	as	PROD_LOST_QTY	,
trnitem.unit_cost	as	PROD_UNIT_COST	,
---trnitem.frm_ref_type	as	REF_DOC_TYP_LKEY	,
trnitem.frm_ref_num	as	REF_DOC_NBR	,
trnitem.frm_ref_line_suf	as	REF_DOC_LN_NBR	,
trnitem.frm_ref_release	as	REF_RELS_NBR	,
trnitem.qty_packed	as	PROD_PKG_QTY	,
trnitem.pick_date	as	ORD_PICK_DTE	,
trnitem.qty_req_conv	as	ON_HAND_BAL_QTY	,
---trnitem.u_m	as	BASE_ON_HAND_BAL_UOM_KEY	,
trnitem.matl_cost	as	PROD_TOT_COST	,
trnitem.lbr_cost	as	LABR_TOT_COST	,
trnitem.fovhd_cost	as	FOH_TOT_COST	,
trnitem.vovhd_cost	as	VOH_TOT_COST	,
trnitem.out_cost	as	OUTSIDE_SVC_TOT_COST	,
trnitem.sch_rcv_date	as	SCHED_RCPT_DTE	,
trnitem.sch_ship_date	as	SCHED_ORD_SHIP_DTE	,
trnitem.unit_price	as	PROD_UNIT_PRC	,

case when length(trim(UPPER((trnitem.pricecode)))) <1 then {{var('default_mapkey')}} when UPPER(trnitem.pricecode)
IS NULL then {{var('default_mapkey')}} else UPPER(trnitem.pricecode) end AS REPLCE_PRC_CD_LKEY,


case when length(trim(UPPER((trnitem.to_ref_type)))) <1 then {{var('default_mapkey')}} when UPPER(trnitem.to_ref_type)
IS NULL then {{var('default_mapkey')}} else UPPER(trnitem.to_ref_type) end AS DEST_REF_SITE_LKEY,


case when length(trim(UPPER((trnitem.to_ref_num)))) <1 then {{var('default_mapkey')}} when UPPER(trnitem.to_ref_num)
IS NULL then {{var('default_mapkey')}} else UPPER(trnitem.to_ref_num) end AS DEST_REF_TYP_LKEY,
---trnitem.pricecode	as	REPLCE_PRC_CD_LKEY	,
---trnitem.to_ref_type	as	DEST_REF_SITE_LKEY	,
---trnitem.to_ref_num	as	DEST_REF_TYP_LKEY	,
trnitem.to_ref_line_suf	as	DEST_REF_LN_NBR	,
trnitem.to_ref_release	as	DEST_REF_DOC_NBR	,

case when length(trim(UPPER((trnitem.from_site)))) <1 then {{var('default_mapkey')}} when UPPER(trnitem.from_site)
IS NULL then {{var('default_mapkey')}} else UPPER(trnitem.from_site) end AS SRC_SITE_KEY,


case when length(trim(UPPER((trnitem.from_whse)))) <1 then {{var('default_mapkey')}} when UPPER(trnitem.from_whse)
IS NULL then {{var('default_mapkey')}} else UPPER(trnitem.from_whse) end AS SRC_WHSE_KEY,
case when length(trim(UPPER((trnitem.to_site)))) <1 then {{var('default_mapkey')}} when UPPER(trnitem.to_site)
IS NULL then {{var('default_mapkey')}} else UPPER(trnitem.to_site) end AS DEST_SITE_KEY,
---trnitem.from_site	as	SRC_SITE_KEY	,
---trnitem.from_whse	as	SRC_WHSE_KEY	,
---trnitem.to_site	as	DEST_SITE_KEY	,
case when length(trim(UPPER((trnitem.to_whse)))) <1 then {{var('default_mapkey')}} when UPPER(trnitem.to_whse)
IS NULL then {{var('default_mapkey')}} else UPPER(trnitem.to_whse) end AS DEST_WHSE_KEY,
case when length(trim(UPPER((trnitem.cross_site)))) <1 then {{var('default_mapkey')}} when UPPER(trnitem.cross_site)
IS NULL then {{var('default_mapkey')}} else UPPER(trnitem.cross_site) end AS CONSOLIDATION_SITE_KEY,
case when length(trim(UPPER((trnitem.comm_code)))) <1 then {{var('default_mapkey')}} when UPPER(trnitem.comm_code)
IS NULL then {{var('default_mapkey')}} else UPPER(trnitem.comm_code) end AS CMDTY_CD_KEY,
---trnitem.to_whse	as	DEST_WHSE_KEY	,
---trnitem.cross_site	as	CONSOLIDATION_SITE_KEY	,
trnitem.unit_freight_cost	as	UNIT_FRT_COST	,
trnitem.unit_freight_cost_conv	as	STD_UNIT_FRT_CONVERTED_COST	,
trnitem.unit_brokerage_cost	as	STD_UNIT_BROKERAGE_CONVERTED_COST	,
trnitem.unit_brokerage_cost_conv	as	STD_UNIT_BROKERAGE_COST	,
trnitem.unit_duty_cost	as	STD_UNIT_PROD_COST	,
trnitem.unit_duty_cost_conv	as	STD_UNIT_DUTY_CONVERTED_COST	,
trnitem.unit_mat_cost	as	UNIT_PROD_COST	,
trnitem.unit_mat_cost_conv	as	STD_UNIT_PROD_CONVERTED_COST	,
trnitem.unit_weight	as	PROD_UNIT_WGT	,
trnitem.lc_override	as	PROD_OVRD_FLAG	,
trnitem.projected_date	as	PROJECTED_DTE	,
---trnitem.comm_code	as	CMDTY_CD_KEY	,
case when length(trim(UPPER((trnitem.process_ind)))) <1 then {{var('default_mapkey')}} when UPPER(trnitem.process_ind)
IS NULL then {{var('default_mapkey')}} else UPPER(trnitem.process_ind) end AS CUST_TAX_TYP_LKEY,
case when length(trim(UPPER((trnitem.delterm)))) <1 then {{var('default_mapkey')}} when UPPER(trnitem.delterm)
IS NULL then {{var('default_mapkey')}} else UPPER(trnitem.delterm) end AS INCOTERMS_KEY,
case when length(trim(UPPER((trnitem.origin)))) <1 then {{var('default_mapkey')}} when UPPER(trnitem.origin)
IS NULL then {{var('default_mapkey')}} else UPPER(trnitem.origin) end AS CNTRY_OF_ORIG_KEY,
---trnitem.process_ind	as	CUST_TAX_TYP_LKEY	,
---trnitem.delterm	as	INCOTERMS_KEY	,
---trnitem.origin	as	CNTRY_OF_ORIG_KEY	,
trnitem.cons_num	as	TRNSFR_IS_EU_CNSGN_FLAG	,
trnitem.export_value	as	PROD_GRS_EXPORT_VAL	,
----trnitem.transport	as	TRNSPRT_MODE_TYP_LKEY	,
trnitem.NoteExistsFlag	as	SLS_ORD_EXIST_FLAG	,
trnitem.RecordDate	as	SLS_LAST_UPD_DTE	,
case when length(trim(UPPER((trnitem.RowPointer)))) <1 then {{var('default_mapkey')}} when UPPER(trnitem.RowPointer)
IS NULL then {{var('default_mapkey')}} else UPPER(trnitem.RowPointer) end AS SLS_DOC_TYP_LKEY,
case when length(trim(UPPER((trnitem.InWorkflow)))) <1 then {{var('default_mapkey')}} when UPPER(trnitem.InWorkflow)
IS NULL then {{var('default_mapkey')}} else UPPER(trnitem.InWorkflow) end AS WORKFLOW_STAT_LKEY,
---trnitem.RowPointer	as	SLS_DOC_TYP_LKEY	,
---trnitem.InWorkflow	as	WORKFLOW_STAT_LKEY	,
trnitem.rcpt_rqmt_q	as	RCPT_REQ_Q_TEXT	,
trnitem.rcpt_rqmt_c	as	RCPT_REQ_C_TEXT	,
trnitem.unit_insurance_cost	as	INS_UNIT_COST	,
trnitem.unit_insurance_cost_conv	as	INS_UNIT_CONVERTED_COST	,
trnitem.unit_loc_frt_cost	as	LCL_FRT_UNIT_COST	,
trnitem.unit_loc_frt_cost_conv	as	LCL_FRT_UNIT_CONVERTED_COST	,
trnitem.suppl_qty_conv_factor	as	SUPPLY_QTY_CONV_FACTOR	,
trnitem.unlinked_xref	as	UNLINKED_XREF_FLAG	,
trnitem.sch_rcv_date_day	as	SCHED_RCV_DY	,
trnitem.sch_ship_date_day	as	SCHED_SHIP_DY	,
trnitem.Uf_PaintDesc	as	PAINT_CD_DESC	,
trnitem.Uf_Paint	as	PAINT_CD_NBR	,
trnitem.Uf_Make	as	ORIG_EQUIP_MAKER_NAME	,
trnitem.Uf_Model	as	ORIG_EQUIP_MODEL_NAME	,
trnitem.preassign_lots	as	PRE_ASGN_LOT_QTY	,
trnitem.preassign_serials	as	PRE_ASGN_SERL_NBR	,
trnitem.Uf_LastPickListUser	as	LAST_PICK_LIST_USER_NAME	,
trnitem.Uf_TakenBy	as	TAKEN_BY_USER_NAME	,
trnitem.Uf_trnitemCreateDate	as	USER_DEFINE_TXN_CREATE_DTE	
FROM {{source('SLIES','TRNITEM')}} TRNITEM