SELECT 
UPPER(CONCAT(coalesce(ITEMLOC.LOC::VARCHAR,''),'~',coalesce(ITEMLOC.WHSE::VARCHAR,'')
,'~',coalesce(ITEMLOC.ITEM::VARCHAR,''),'~',coalesce(ITEMLOC.site_ref::VARCHAR,''))) as	INVTY_ON_HAND_SNAPSHOT_KEY	,
'SLIES' AS {{var('column_srcsyskey')}},
MD5(UPPER(CONCAT(coalesce(ITEMLOC.LOC::VARCHAR,''),'~',coalesce(ITEMLOC.WHSE::VARCHAR,'')
,'~',coalesce(ITEMLOC.ITEM::VARCHAR,''),'~',coalesce(ITEMLOC.site_ref::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
ITEMLOC.LOADDTS AS {{var('column_z3loddtm')}},
ITEMLOC.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
itemloc.rank	as	PROD_VELOCITY_RANK_NBR	,

case when length(trim(UPPER((itemloc.loc)))) <1 then {{var('default_mapkey')}} when UPPER(itemloc.loc)
IS NULL then {{var('default_mapkey')}} else UPPER(itemloc.loc) end AS SUB_LOC_KEY,

---itemloc.loc	as	SUB_LOC_KEY	,
itemloc.qty_on_hand	as	PROD_ON_HAND_QTY	,
itemloc.inv_acct	as	GL_ACCT_NBR	,
itemloc.unit_cost	as	PROD_UNIT_COST	,
itemloc.matl_cost	as	PROD_COST	,
itemloc.lbr_acct	as	LABR_ACCT_NBR	,
itemloc.fovhd_acct	as	FOH_ACCT_NBR	,
itemloc.vovhd_acct	as	VOH_ACCT_NBR	,
itemloc.out_acct	as	SUB_CONTRACTING_ACCTG_NBR	,
itemloc.inv_acct_unit2	as	GL_ACCT_2_NBR	,
itemloc.inv_acct_unit4	as	GL_ACCT_4_NBR	,
itemloc.lbr_acct_unit2	as	LABR_ACCT_2_NBR	,
itemloc.lbr_acct_unit4	as	LABR_ACCT_4_NBR	,
itemloc.fovhd_acct_unit2	as	FOH_ACCT_2_NBR	,
itemloc.fovhd_acct_unit4	as	FOH_ACCT_3_NBR	,
itemloc.vovhd_acct_unit2	as	VOH_ACCT_2_NBR	,
itemloc.vovhd_acct_unit4	as	VOH_ACCT_4_NBR	,
itemloc.out_acct_unit2	as	SUB_CONTRACTING_ACCTG_2_NBR	,
itemloc.out_acct_unit4	as	SUB_CONTRACTING_ACCTG_4_NBR	,
itemloc.RowPointer	as	RCRD_NBR	,
itemloc.NewRank	as	PROD_UPD_RANK_NBR	,
itemloc.last_count_qty_on_hand	as	PROD_LAST_ON_HAND_QTY	,
itemloc.assigned_to_be_picked_qty	as	ASGN_PICK_QTY	,

case when length(trim(UPPER((itemloc.whse)))) <1 then {{var('default_mapkey')}} when UPPER(itemloc.whse)
IS NULL then {{var('default_mapkey')}} else UPPER(itemloc.whse) end AS STORE_LOC_KEY,


---itemloc.whse	as	STORE_LOC_KEY	,


case when length(trim(UPPER((itemloc.item)))) <1 then {{var('default_mapkey')}} when UPPER(itemloc.item)
IS NULL then {{var('default_mapkey')}} else UPPER(itemloc.item) end AS PROD_KEY,

---itemloc.item	as	PROD_KEY	,
itemloc.mrb_flag	as	NON_NETTABLE_FLAG	,
---itemloc.loc_type	as	LOC_TYP_LKEY	, --- COLUMN MISSING IN FINAL TABLE
itemloc.perm_flag	as	PERM_FLAG	,
itemloc.qty_rsvd	as	RESERVED_QTY	,
itemloc.lbr_cost	as	LABR_COST	,
itemloc.fovhd_cost	as	FOH_COST	,
itemloc.vovhd_cost	as	VOH_COST	,
itemloc.out_cost	as	OUTSIDE_COST	,
itemloc.wc	as	WORK_CNTR_NAME	,
itemloc.inv_acct_unit1	as	PROD_ACCT_UNIT_1	,
itemloc.inv_acct_unit3	as	PROD_ACCT_UNIT_3	,
itemloc.lbr_acct_unit1	as	LABR_ACCT_UNIT_1	,
itemloc.lbr_acct_unit3	as	LABR_ACCT_UNIT_3	,
itemloc.fovhd_acct_unit1	as	FOH_ACCT_UNIT_1	,
itemloc.fovhd_acct_unit3	as	FOH_ACCT_UNIT_3	,
itemloc.vovhd_acct_unit1	as	VOH_ACCT_UNIT_1	,
itemloc.vovhd_acct_unit3	as	VOH_ACCT_UNIT_3	,
itemloc.out_acct_unit1	as	OUTSIDE_ACCT_UNIT_1	,
itemloc.out_acct_unit3	as	OUTSIDE_ACCT_UNIT_3	,

case when length(trim(UPPER((itemloc.site_ref)))) <1 then {{var('default_mapkey')}} when UPPER(itemloc.site_ref)
IS NULL then {{var('default_mapkey')}} else UPPER(itemloc.site_ref) end AS LOC_KEY

--itemloc.site_ref	as	LOC_KEY	
FROM {{source('SLIES','ITEMLOC')}} ITEMLOC