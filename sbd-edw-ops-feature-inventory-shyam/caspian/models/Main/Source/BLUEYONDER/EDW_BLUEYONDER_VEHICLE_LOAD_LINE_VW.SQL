SELECT     
UPPER(CONCAT(COALESCE(VEHICLELOADLINE.LOADID::VARCHAR,''),'~',COALESCE(VEHICLELOADLINE.PRIMARYITEM::VARCHAR,''), '~', COALESCE(VEHICLELOADLINE.EXPDATE::VARCHAR,''),
'~', COALESCE(VEHICLELOADLINE.ITEM::VARCHAR,''),'~', COALESCE(VEHICLELOADLINE.DEST::VARCHAR,''))) AS VHCL_LOD_LN_KEY,
'BLUEYONDER' as  {{var('column_srcsyskey')}},  
MD5(UPPER(CONCAT(COALESCE(VEHICLELOADLINE.LOADID::VARCHAR,''),'~',COALESCE(VEHICLELOADLINE.PRIMARYITEM::VARCHAR,''), '~', COALESCE(VEHICLELOADLINE.EXPDATE::VARCHAR,''),
'~', COALESCE(VEHICLELOADLINE.ITEM::VARCHAR,''),'~', COALESCE(VEHICLELOADLINE.DEST::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} as {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' as {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ as {{var('column_ETL_INS_DTE')}},
'{{model.name}}' as {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ as {{var('column_ETL_UPD_DTE')}},
VEHICLELOADLINE.LOADDTS as {{var('column_z3loddtm')}},
VEHICLELOADLINE.LOADDTS as  {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' as {{var('column_verexpirydt')}},
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
VEHICLELOADLINE.ACTION	AS	LOD_ACTN_PERFORM_TYP,
--VEHICLELOADLINE.ACTIONALLOWEDSW	AS	VHCL_LOD_SUGGESTED_ACTN_ALLOW_FLAG,
VEHICLELOADLINE.ACTIONDATE	AS	VHCL_LOD_SUGGESTED_SUPPLY_DTE,
VEHICLELOADLINE.ACTIONQTY	AS	VHCL_LOD_SUGGESTED_SUPPLY_QTY,
VEHICLELOADLINE.DEST	AS	DEST_LOC_KEY,
VEHICLELOADLINE.DRAWQTY	AS	DRAW_QTY,
VEHICLELOADLINE.EXPDATE	AS	SKU_EXPR_DTE,
VEHICLELOADLINE.GROUPORDERID	AS	VHCL_LOD_GRP_ORD_ID,
VEHICLELOADLINE.ITEM	AS	PROD_KEY,
VEHICLELOADLINE.LBSOURCE	AS	VHCL_LOD_ORIG_STAT,
VEHICLELOADLINE.LOADID	AS	VHCL_LOD_ID,
VEHICLELOADLINE.LOADLINEID	AS	LOD_LN_NBR,
VEHICLELOADLINE.MUSTGOQTY	AS	MUST_GO_QTY,
VEHICLELOADLINE.ORDERID	AS	VHCL_LOD_ORD_NBR,
VEHICLELOADLINE.ORDERNUM	AS	VHCL_LOD_LN_ORD_NBR,
VEHICLELOADLINE.PRIMARYITEM	AS	DMD_PROD_KEY,
VEHICLELOADLINE.PRIMARYITEMQTY	AS	DMD_PRIM_QTY,
VEHICLELOADLINE.PROJECT	AS	VHCL_LOD_PROJ_NBR,
VEHICLELOADLINE.QTY	AS	PROD_QTY,
VEHICLELOADLINE.REVISEDDATE	AS	REVISED_DTE,
VEHICLELOADLINE.REVISEDEXPDATE	AS	REVISED_EXPR_DTE,
VEHICLELOADLINE.SCHEDARRIVDATE	AS	SCHED_ARRVL_DTE,
VEHICLELOADLINE.SCHEDSHIPDATE	AS	LOD_SCHED_SHIP_DTE,
VEHICLELOADLINE.SEQNUM	AS	LOD_LN_SEQ_NBR,
VEHICLELOADLINE.SOURCE	AS	SRC_LOC,
VEHICLELOADLINE.SOURCING	AS	SRC_MTHD_NAME,
VEHICLELOADLINE.SUPPLYID	AS	SUPPLY_ID,
VEHICLELOADLINE.SUPPLYMETHOD	AS	SUPPLY_MTHD,
VEHICLELOADLINE.SUPPORDERQTY	AS	SUPL_ORD_QTY,
case when VEHICLELOADLINE.ACTIONALLOWEDSW IS NULL then 'U'
when VEHICLELOADLINE.ACTIONALLOWEDSW in('YES','TRUE','1.0000000000') then 'Y'
when VEHICLELOADLINE.ACTIONALLOWEDSW in('NO','FALSE','0') then 'N'
else '' end as VHCL_LOD_SUGGESTED_ACTN_ALLOW_FLAG
FROM {{source('BLUEYONDER','VEHICLELOADLINE')}} VEHICLELOADLINE