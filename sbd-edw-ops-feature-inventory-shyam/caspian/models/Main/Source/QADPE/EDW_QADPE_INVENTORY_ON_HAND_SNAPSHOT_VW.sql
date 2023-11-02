SELECT
'QADPE' AS {{var('column_srcsyskey')}},
UPPER(CONCAT(coalesce(LDDET.ld_part::VARCHAR,''),'~',coalesce(LDDET.ld_site::VARCHAR,''),'~',coalesce(LDDET.ld_loc::VARCHAR,'#'),'~',coalesce(LDDET.ld_lot::VARCHAR,'#'),'~',coalesce(LDDET.ld_ref::VARCHAR,'#'))) as INVTY_ON_HAND_SNAPSHOT_KEY,
MD5(UPPER(CONCAT(coalesce(LDDET.ld_part::VARCHAR,''),'~',coalesce(LDDET.ld_site::VARCHAR,''),'~',coalesce(LDDET.ld_loc::VARCHAR,'#'),'~',coalesce(LDDET.ld_lot::VARCHAR,'#'),'~',coalesce(LDDET.ld_ref::VARCHAR,'#')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
LDDET.LOADDTS AS {{var('column_z3loddtm')}},
LDDET.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
case when length(trim(UPPER((LDDET.ld_part)))) <1 then {{var('default_mapkey')}} when UPPER(LDDET.ld_part)
IS NULL then {{var('default_mapkey')}} else UPPER(LDDET.ld_part) end as PROD_KEY,
case when length(trim(UPPER((LDDET.ld_site)))) <1 then {{var('default_mapkey')}} when UPPER(LDDET.ld_site)
IS NULL then {{var('default_mapkey')}} else UPPER(LDDET.ld_site) end as LOC_KEY,
UPPER(LDDET.ld_loc) as SLOC_ID,
LDDET.ld_qty_oh	as	PROD_NET_ON_HAND_QTY	,
CASE WHEN LDDET.ld_expire='?' THEN null ELSE TRY_TO_DATE(LDDET.ld_expire,'MM/DD/YY') END as PROD_EXPIRY_DTE	,
UPPER(LDDET.ld_lot)	as	STORG_BIN_ID	,
case when length(trim(LDDET.ld_status))<1 then {{var('default_mapkey')}} when LDDET.ld_status IS NULL then  {{var('default_mapkey')}} else upper(LDDET.ld_status) end AS INVTY_WHSE_USE_STK_STAT_LKEY
FROM {{source('QADPE','LDDET')}} LDDET


