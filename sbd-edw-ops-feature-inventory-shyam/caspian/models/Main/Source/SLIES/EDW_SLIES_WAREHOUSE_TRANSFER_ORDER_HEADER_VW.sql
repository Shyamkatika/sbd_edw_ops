select
UPPER(CONCAT(coalesce(TRANSFER.SITE_REF::VARCHAR,''),'~',coalesce(TRANSFER.trn_num::VARCHAR,''))) AS WHSE_TRNSFR_ORD_HDR_KEY,
'SLIES' AS {{var('column_srcsyskey')}},
MD5(UPPER(CONCAT(coalesce(TRANSFER.SITE_REF::VARCHAR,''),'~',coalesce(TRANSFER.trn_num::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
TRANSFER.LOADDTS AS {{var('column_z3loddtm')}},
TRANSFER.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},


case when length(trim(UPPER((TRANSFER.from_whse)))) <1 then {{var('default_mapkey')}} when UPPER(TRANSFER.from_whse)
IS NULL then {{var('default_mapkey')}} else UPPER(TRANSFER.from_whse) end AS ORIG_WHSE_KEY,

case when length(trim(UPPER((TRANSFER.to_whse)))) <1 then {{var('default_mapkey')}} when UPPER(TRANSFER.to_whse)
IS NULL then {{var('default_mapkey')}} else UPPER(TRANSFER.to_whse) end AS DEST_WHSE_KEY,

case when length(trim(UPPER((TRANSFER.from_site)))) <1 then {{var('default_mapkey')}} when UPPER(TRANSFER.from_site)
IS NULL then {{var('default_mapkey')}} else UPPER(TRANSFER.from_site) end AS PYMT_POST_ORIG_SITE_KEY,

case when length(trim(UPPER((TRANSFER.to_site)))) <1 then {{var('default_mapkey')}} when UPPER(TRANSFER.to_site)
IS NULL then {{var('default_mapkey')}} else UPPER(TRANSFER.to_site) end AS PYMT_POST_DEST_SITE_KEY,

case when length(trim(UPPER((TRANSFER.entered_site)))) <1 then {{var('default_mapkey')}} when UPPER(TRANSFER.entered_site)
IS NULL then {{var('default_mapkey')}} else UPPER(TRANSFER.entered_site) end AS PROD_ENTR_SITE_KEY,

case when length(trim(UPPER((TRANSFER.fob_site)))) <1 then {{var('default_mapkey')}} when UPPER(TRANSFER.fob_site)
IS NULL then {{var('default_mapkey')}} else UPPER(TRANSFER.fob_site) end AS FRT_ON_BOARD_SITE_KEY,

case when length(trim(UPPER((TRANSFER.freight_vendor)))) <1 then {{var('default_mapkey')}} when UPPER(TRANSFER.freight_vendor)
IS NULL then {{var('default_mapkey')}} else UPPER(TRANSFER.freight_vendor) end AS FRT_VEND_KEY,

case when length(trim(UPPER((TRANSFER.SITE_REF)))) <1 then {{var('default_mapkey')}} when UPPER(TRANSFER.SITE_REF)
IS NULL then {{var('default_mapkey')}} else UPPER(TRANSFER.SITE_REF) end AS LOC_KEY,



case when length(trim(UPPER((TRANSFER.stat)))) <1 then {{var('default_mapkey')}} when UPPER(TRANSFER.stat)
IS NULL then {{var('default_mapkey')}} else UPPER(TRANSFER.stat) end AS TRNSFR_ORD_STAT_LKEY,

case when length(trim(UPPER((TRANSFER.duty_alloc_meth)))) <1 then {{var('default_mapkey')}} when UPPER(TRANSFER.duty_alloc_meth)
IS NULL then {{var('default_mapkey')}} else UPPER(TRANSFER.duty_alloc_meth) end AS DUTY_ALLOC_MTHD_LKEY,

case when length(trim(UPPER((TRANSFER.frt_alloc_meth)))) <1 then {{var('default_mapkey')}} when UPPER(TRANSFER.frt_alloc_meth)
IS NULL then {{var('default_mapkey')}} else UPPER(TRANSFER.frt_alloc_meth) end AS FRT_ALLOC_MTHD_LKEY,

case when length(trim(UPPER((TRANSFER.brk_alloc_meth)))) <1 then {{var('default_mapkey')}} when UPPER(TRANSFER.brk_alloc_meth)
IS NULL then {{var('default_mapkey')}} else UPPER(TRANSFER.brk_alloc_meth) end AS BREAK_ALLOC_MTHD_LKEY,

case when length(trim(UPPER((TRANSFER.duty_alloc_type)))) <1 then {{var('default_mapkey')}} when UPPER(TRANSFER.duty_alloc_type)
IS NULL then {{var('default_mapkey')}} else UPPER(TRANSFER.duty_alloc_type) end AS DUTY_ALLOC_TYP_LKEY,

case when length(trim(UPPER((TRANSFER.frt_alloc_type)))) <1 then {{var('default_mapkey')}} when UPPER(TRANSFER.frt_alloc_type)
IS NULL then {{var('default_mapkey')}} else UPPER(TRANSFER.frt_alloc_type) end AS FRT_ALLOC_TYP_LKEY,

case when length(trim(UPPER((TRANSFER.brk_alloc_type)))) <1 then {{var('default_mapkey')}} when UPPER(TRANSFER.brk_alloc_type)
IS NULL then {{var('default_mapkey')}} else UPPER(TRANSFER.brk_alloc_type) end AS BREAK_ALLOC_TYP_LKEY,

case when length(trim(UPPER((TRANSFER.trans_nat)))) <1 then {{var('default_mapkey')}} when UPPER(TRANSFER.trans_nat)
IS NULL then {{var('default_mapkey')}} else UPPER(TRANSFER.trans_nat) end AS PICK_PACKED_STAT_LKEY,

case when length(trim(UPPER((TRANSFER.process_ind)))) <1 then {{var('default_mapkey')}} when UPPER(TRANSFER.process_ind)
IS NULL then {{var('default_mapkey')}} else UPPER(TRANSFER.process_ind) end AS EU_TAX_TYP_LKEY,

case when length(trim(UPPER((TRANSFER.delterm)))) <1 then {{var('default_mapkey')}} when UPPER(TRANSFER.delterm)
IS NULL then {{var('default_mapkey')}} else UPPER(TRANSFER.delterm) end AS INCOTERMS_LKEY,

case when length(trim(UPPER((TRANSFER.RowPointer)))) <1 then {{var('default_mapkey')}} when UPPER(TRANSFER.RowPointer)
IS NULL then {{var('default_mapkey')}} else UPPER(TRANSFER.RowPointer) end AS SLS_RCRD_TYP_LKEY,

case when length(trim(UPPER((TRANSFER.InWorkflow)))) <1 then {{var('default_mapkey')}} when UPPER(TRANSFER.InWorkflow)
IS NULL then {{var('default_mapkey')}} else UPPER(TRANSFER.InWorkflow) end AS WORKFLOW_STAT_LKEY,

case when length(trim(UPPER((TRANSFER.ins_alloc_type)))) <1 then {{var('default_mapkey')}} when UPPER(TRANSFER.ins_alloc_type)
IS NULL then {{var('default_mapkey')}} else UPPER(TRANSFER.ins_alloc_type) end AS INS_ALLOC_TYP_LKEY,

case when length(trim(UPPER((TRANSFER.ins_alloc_meth)))) <1 then {{var('default_mapkey')}} when UPPER(TRANSFER.ins_alloc_meth)
IS NULL then {{var('default_mapkey')}} else UPPER(TRANSFER.ins_alloc_meth) end AS INS_ALLOC_MTHD_LKEY,

case when length(trim(UPPER((TRANSFER.loc_frt_alloc_type)))) <1 then {{var('default_mapkey')}} when UPPER(TRANSFER.loc_frt_alloc_type)
IS NULL then {{var('default_mapkey')}} else UPPER(TRANSFER.loc_frt_alloc_type) end AS LCL_FRT_ALLOC_TYP_LKEY,

case when length(trim(UPPER((TRANSFER.loc_frt_alloc_meth)))) <1 then {{var('default_mapkey')}} when UPPER(TRANSFER.loc_frt_alloc_meth)
IS NULL then {{var('default_mapkey')}} else UPPER(TRANSFER.loc_frt_alloc_meth) end AS FRT_ALLOC_COST_MTHD_LKEY,

case when length(trim(UPPER((TRANSFER.export_type)))) <1 then {{var('default_mapkey')}} when UPPER(TRANSFER.export_type)
IS NULL then {{var('default_mapkey')}} else UPPER(TRANSFER.export_type) end AS EXPORT_TYP_LKEY,



case when UPPER(TRANSFER.NoteExistsFlag) in ('NO','FALSE','0') or UPPER(TRANSFER.NoteExistsFlag) is null
or length(TRIM(TRANSFER.NoteExistsFlag))<1 then 'N' when UPPER(TRANSFER.NoteExistsFlag) in ('YES','TRUE','1')
then 'Y' else 'U' end as PROD_TRNSFR_RQST_FLAG,

case when UPPER(TRANSFER.fs_mobile_transfer_match) in ('NO','FALSE','0') or UPPER(TRANSFER.fs_mobile_transfer_match) is null
or length(TRIM(TRANSFER.fs_mobile_transfer_match))<1 then 'N' when UPPER(TRANSFER.fs_mobile_transfer_match) in ('YES','TRUE','1')
then 'Y' else 'U' end as MOBILE_TRNSFR_MATCH_FLAG,

case when UPPER(TRANSFER.external_whse_line_changed) in ('NO','FALSE','0') or UPPER(TRANSFER.external_whse_line_changed) is null
or length(TRIM(TRANSFER.external_whse_line_changed))<1 then 'N' when UPPER(TRANSFER.external_whse_line_changed) in ('YES','TRUE','1')
then 'Y' else 'U' end as EXTNL_WHSE_LN_CHG_FLAG,


TRANSFER.RecordDate AS SLS_LAST_UPD_DTE,
TRANSFER.order_date AS PROD_TRNSFR_ORD_DTE,


TRANSFER.ship_code as SHIP_ORIG_PORT_NBR,
TRANSFER.weight as PROD_WGT,
TRANSFER.qty_packages AS PROD_PKG_QTY,
TRANSFER.exch_rate AS EXCH_RATE,
TRANSFER.duty_vendor AS DUTY_VEND_NAME,
TRANSFER.brokerage_vendor AS VEND_NAME,
TRANSFER.frt_alloc_percent AS PROD_FRT_COST_PCT,
TRANSFER.duty_alloc_percent AS PROD_DUTY_CHRG_COST_PCT,
TRANSFER.brk_alloc_percent AS PROD_BROKERAGE_CHRG_COST_PCT,
TRANSFER.est_freight_amt AS EST_FRT_QTY,
TRANSFER.act_freight_amt AS ACT_FRT_QTY,
TRANSFER.est_brokerage_amt AS EST_BROKERAGE_AMT,
TRANSFER.act_brokerage_amt AS ACT_BROKERAGE_AMT,
TRANSFER.est_duty_amt AS EST_DUTY_AMT,
TRANSFER.act_duty_amt AS ACT_TRNSFR_DUTY_AMT,
TRANSFER.freight_amt_t AS ACT_SHIP_FRT_AMT,
TRANSFER.brokerage_amt_t AS BROKERAGE_AMT,
TRANSFER.duty_amt_t AS ACT_SHIP_DUTY_AMT,
TRANSFER.ins_vendor AS INS_AGENT_NAME,
TRANSFER.ins_alloc_percent AS INS_ALLOC_PCT,
TRANSFER.est_insurance_amt AS EST_INS_QTY,
TRANSFER.act_insurance_amt AS ACT_INS_QTY,
TRANSFER.insurance_amt_t AS INS_AMT,
TRANSFER.loc_frt_vendor AS LCL_FRT_SVC_PROVIDER_NAME,
TRANSFER.loc_frt_alloc_percent AS LCL_FRT_ALLOC_PCT,
TRANSFER.est_local_freight_amt AS EST_FRT_AMT,
TRANSFER.act_local_freight_amt AS ACT_LCL_FRT_AMT,
TRANSFER.local_freight_amt_t AS LCL_FRT_COST,
TRANSFER.trans_nat_2 AS SCNDRY_NOTC_NBR,
TRANSFER.fs_mobile_transfer_lot AS LOT_TRNSFR_NBR,
TRANSFER.fs_mobile_transfer_ser_num AS TRNSFR_PROD_SERL_NBR,
TRANSFER.ssspj_delivery_instr AS DLV_INSTRC_TEXT,
TRANSFER.trn_num AS TRNSFR_ORD_NBR

FROM {{source('SLIES','TRANSFER')}} TRANSFER