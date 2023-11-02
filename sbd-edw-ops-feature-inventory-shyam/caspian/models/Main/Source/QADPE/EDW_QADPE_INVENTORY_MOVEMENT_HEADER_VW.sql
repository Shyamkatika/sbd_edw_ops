SELECT
'QADPE' AS {{var('column_srcsyskey')}},
UPPER(CONCAT(coalesce(PTPDET.ptp_part::VARCHAR,''),'~',coalesce(PTPDET.ptp_site::VARCHAR,''))) as INVTY_MVMNT_HDR_KEY,
MD5(UPPER(CONCAT(coalesce(PTPDET.ptp_part::VARCHAR,''),'~',coalesce(PTPDET.ptp_site::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
PTPDET.LOADDTS AS {{var('column_z3loddtm')}},PTPDET.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
case when length(trim(UPPER((PTPDET.ptp_part)))) <1 then {{var('default_mapkey')}} when UPPER(PTPDET.ptp_part)
IS NULL then {{var('default_mapkey')}} else UPPER(PTPDET.ptp_part) end AS PROD_KEY,
PTPDET.ptp_batch	as	BASE_QTY	,
case when length(trim(UPPER((PTPDET.ptp_bom_code)))) <1 then {{var('default_mapkey')}} when UPPER(PTPDET.ptp_bom_code)
IS NULL then {{var('default_mapkey')}} else UPPER(PTPDET.ptp_bom_code) end AS BOM_KEY,
----PTPDET.ptp_bom_code	as	BOM_KEY	,
PTPDET.ptp_cum_lead	as	PROD_LEADNUM	, --CHK
PTPDET.ptp_ins_lead	as	PUR_INSPCT_NUM	, --CHK

CASE WHEN UPPER(PTPDET.ptp_ins_rqd)='NO' then 'N' WHEN UPPER(PTPDET.ptp_ins_rqd)='YES' then 'Y'
 	ELSE 'U' end as	INCM_INSPCT_FLAG	,
CASE WHEN UPPER(PTPDET.ptp_iss_pol)='NO' then 'N' WHEN UPPER(PTPDET.ptp_iss_pol)='YES' then 'Y'
 	ELSE 'U' end as	PICK_LIST_FLAG,

PTPDET.ptp_mfg_lead	as	PROD_MFG_DY_CNT	,
CASE WHEN UPPER(PTPDET.ptp_ms)='NO' then 'N' WHEN UPPER(PTPDET.ptp_ms)='YES' then 'Y'
 	ELSE 'U' end as	PROD_APPEARED_IN_SCHED_FLAG,
PTPDET.ptp_ord_max	as	PROD_MAX_ORD_QTY	,
PTPDET.ptp_ord_min	as	PROD_MIN_ORD_QTY	,
PTPDET.ptp_ord_mult	as	PROD_LOT_ORD_QTY	,
PTPDET.ptp_ord_per	as	ORD_POLICY_DMD_DY_CNT	,
case when length(trim(UPPER((PTPDET.ptp_ord_pol)))) <1 then {{var('default_mapkey')}} when UPPER(PTPDET.ptp_ord_pol)
IS NULL then {{var('default_mapkey')}} else UPPER(PTPDET.ptp_ord_pol) end AS MRP_ORD_POLICY_LKEY,
----PTPDET.ptp_ord_pol	as	MRP_ORD_POLICY_LKEY	,
PTPDET.ptp_ord_qty	as	FOQ_ORD_POLICY_QTY	,
CASE WHEN UPPER(PTPDET.ptp_phantom)='NO' then 'N' WHEN UPPER(PTPDET.ptp_phantom)='YES' then 'Y'
 	ELSE 'U' end as	PHNTM_FLAG	,
CASE WHEN UPPER(PTPDET.ptp_plan_ord)='NO' then 'N' WHEN UPPER(PTPDET.ptp_plan_ord)='YES' then 'Y'
 	ELSE 'U' end as	PLND_ORD_FLAG	,
	 case when length(trim(UPPER((PTPDET.ptp_pm_code)))) <1 then {{var('default_mapkey')}} when UPPER(PTPDET.ptp_pm_code)
IS NULL then {{var('default_mapkey')}} else UPPER(PTPDET.ptp_pm_code) end AS PROD_PUR_MFG_LKEY,
PTPDET.ptp_pur_lead	as	CMPLT_PO_DY_CNT	,
case when length(trim(UPPER((PTPDET.ptp_rev)))) <1 then {{var('default_mapkey')}} when UPPER(PTPDET.ptp_rev)
IS NULL then {{var('default_mapkey')}} else UPPER(PTPDET.ptp_rev) end AS PROD_ENGNR_RVN_LVL_LKEY,
PTPDET.ptp_rop	as	REORD_RPT_QTY	,
PTPDET.ptp_routing	as	PROD_RTG_NBR	,
PTPDET.ptp_run	as	PROD_UNIT_STD_RUN_HRS_CNT	,
PTPDET.ptp_run_ll	as	LOWER_LVL_ACCUM_RUN_NUM	,
PTPDET.ptp_setup	as	PROD_SET_UP_STD_NUM	,
PTPDET.ptp_setup_ll	as	LOWER_LVL_ACCUM_SETUP_NUM	,
PTPDET.ptp_sfty_stk	as	PROD_MIN_INVTY_QTY	,
PTPDET.ptp_sfty_tme	as	PRIOR_DYS_FOR_DUE_DTE_CNT	,
PTPDET.ptp_timefnce	as	NO_PLND_ORD_CREATE_DY_CNT	,
PTPDET.ptp_user1	as	MOD_BY_USER_1_NAME	,
PTPDET.ptp_user2	as	MOD_BY_USER_2_NAME	,
PTPDET.ptp_vend     AS  PRIM_SUPLR_ADDR_TYP,
PTPDET.ptp_yld_pct	as	PROD_ORD_ACCEPTABLE_PCT	,
PTPDET.ptp_assay	as	PROD_NORMAL_ASSAY_PCT	,
case when length(trim(UPPER((PTPDET.ptp_grade)))) <1 then {{var('default_mapkey')}} when UPPER(PTPDET.ptp_grade)
IS NULL then {{var('default_mapkey')}} else UPPER(PTPDET.ptp_grade) end AS PROD_INVTY_GRADE_LKEY,
case when length(trim(UPPER((PTPDET.ptp_ll_bom)))) <1 then {{var('default_mapkey')}} when UPPER(PTPDET.ptp_ll_bom)
IS NULL then {{var('default_mapkey')}} else UPPER(PTPDET.ptp_ll_bom) end AS CALC_SEQ_LKEY,
CASE WHEN UPPER(PTPDET.ptp_eco_pend)='NO' then 'N' WHEN UPPER(PTPDET.ptp_eco_pend)='YES' then 'Y'
 	ELSE 'U' end as	ENGNR_CHG_FLAG	,
CASE WHEN UPPER(PTPDET.ptp_rollup)='NO' then 'N' WHEN UPPER(PTPDET.ptp_rollup)='YES' then 'Y'
 	ELSE 'U' end as	BOM_ROLL_UP_FLAG,
	 -----TRY_TO_DATE(MARD.DLINL,'YYYYMMDD')
TRY_TO_DATE(ptp_Added,'MM/DD/YY')	as	PROD_SYS_ADD_DTE	,
case when length(trim(UPPER((PTPDET.ptp_draw)))) <1 then {{var('default_mapkey')}} when UPPER(PTPDET.ptp_draw)
IS NULL then {{var('default_mapkey')}} else UPPER(PTPDET.ptp_draw) end AS PROD_DRAWING_ID_LKEY,
case when length(trim(UPPER((PTPDET.ptp_trantype)))) <1 then {{var('default_mapkey')}} when UPPER(PTPDET.ptp_trantype)
IS NULL then {{var('default_mapkey')}} else UPPER(PTPDET.ptp_trantype) end AS TRNSPRT_TYP_LKEY,
PTPDET.ptp_chr01	as	USER_DEFINE_CUSTOM_1_TEXT	,
PTPDET.ptp_chr02	as	USER_DEFINE_CUSTOM_2_TEXT	,
PTPDET.ptp_chr03	as	USER_DEFINE_CUSTOM_3_TEXT	,
PTPDET.ptp_chr04	as	USER_DEFINE_CUSTOM_4_TEXT	,
PTPDET.ptp_chr05	as	USER_DEFINE_CUSTOM_5_TEXT	,
PTPDET.ptp_chr06	as	USER_DEFINE_CUSTOM_6_TEXT	,
PTPDET.ptp_chr07	as	USER_DEFINE_CUSTOM_7_TEXT	,
PTPDET.ptp_chr08	as	USER_DEFINE_CUSTOM_8_TEXT	,
PTPDET.ptp_chr09	as	USER_DEFINE_CUSTOM_9_TEXT	,
PTPDET.ptp_chr10	as	USER_DEFINE_CUSTOM_10_TEXT	,
TRY_TO_DATE(PTPDET.ptp_dte01,'YYYYMMDD')	as	USER_DEFINE_CUSTOM_1_DTE	, ---CHK
TRY_TO_DATE(PTPDET.ptp_dte02,'YYYYMMDD')	as	USER_DEFINE_CUSTOM_2_DTE	, ---CHK
PTPDET.ptp_dec01	as	USER_DEFINE_CUSTOM_1_NBR	,
PTPDET.ptp_dec02	as	USER_DEFINE_CUSTOM_2_NBR	,
PTPDET.ptp_log01	as	USER_DEFINE_LOGICAL_1_NBR	,
PTPDET.ptp_log02	as	USER_DEFINE_LOGICAL_2_NBR	,
PTPDET.ptp_ll_drp	as	LOWER_LVL_DIST_NETWORK_NBR	,
case when length(trim(UPPER((PTPDET.ptp_po_site)))) <1 then {{var('default_mapkey')}} when UPPER(PTPDET.ptp_po_site)
IS NULL then {{var('default_mapkey')}} else UPPER(PTPDET.ptp_po_site) end AS PUR_LOC_KEY,
PTPDET.ptp_network	as	BOM_NETWORK_NBR	,
PTPDET.ptp_mfg_pct	as	MFG_PLND_ORD_PCT	,
PTPDET.ptp_pur_pct	as	PUR_PLND_ORD_PCT	,
PTPDET.ptp_drp_pct	as	DRP_SUPPLIED_PLND_ORD_PCT	,
case when length(trim(UPPER((PTPDET.ptp_pou_code)))) <1 then {{var('default_mapkey')}} when UPPER(PTPDET.ptp_pou_code)
IS NULL then {{var('default_mapkey')}} else UPPER(PTPDET.ptp_pou_code) end AS POINT_OF_USE_LKEY,
PTPDET.ptp_wks_avg	as	FCSTD_AVG_WKS_CNT	,
PTPDET.ptp_wks_max	as	FCSTD_MAX_WKS_CNT	,
PTPDET.ptp_wks_min	as	FCSTD_MIN_WKS_CNT	,
case when length(trim(UPPER((PTPDET.ptp_pick_logic)))) <1 then {{var('default_mapkey')}} when UPPER(PTPDET.ptp_pick_logic)
IS NULL then {{var('default_mapkey')}} else UPPER(PTPDET.ptp_pick_logic) end AS PICK_LOGIC_LKEY,
case when length(trim(UPPER((PTPDET.ptp_site)))) <1 then {{var('default_mapkey')}} when UPPER(PTPDET.ptp_site)
IS NULL then {{var('default_mapkey')}} else UPPER(PTPDET.ptp_site) end AS LOC_KEY,
PTPDET.ptp_buyer	as	PROD_RESP_PRSN_NAME	
FROM {{source('QADPE','PTPDET')}} PTPDET