SELECT
UPPER(CONCAT(COALESCE(TRIM(PLANARRIV.SEQNUM)::VARCHAR,''),'~',COALESCE(TRIM(PLANARRIV.ITEM)::VARCHAR,''),'~', 
COALESCE(TRIM(PLANARRIV.DEST)::VARCHAR,''))) AS SUPPLY_PLN_ARRVL_KEY,
'BLUEYONDER' AS  {{var('column_srcsyskey')}},  
MD5(UPPER(CONCAT(COALESCE(TRIM(PLANARRIV.SEQNUM)::VARCHAR,''),'~',COALESCE(TRIM(PLANARRIV.ITEM)::VARCHAR,''),'~', 
COALESCE(TRIM(PLANARRIV.DEST)::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
PLANARRIV.LOADDTS AS  {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
PLANARRIV.LOADDTS AS {{var('column_z3loddtm')}},
PLANARRIV.ACTION	AS	ACTN_TYP,
case when PLANARRIV.ACTIONALLOWEDSW IS NULL then 'U' when UPPER(PLANARRIV.ACTIONALLOWEDSW) in ('YES','TRUE','1') then 'Y'
when UPPER(PLANARRIV.ACTIONALLOWEDSW) in ('NO','FALSE','0') then 'N' else 'U' end as ACTN_ALLOW_FLAG,
PLANARRIV.ACTIONDATE	AS	ACTN_DTE,
to_number(PLANARRIV.ACTIONQTY,28,10) AS ACTN_QTY,
PLANARRIV.AVAILTOSHIPDATE	AS	AVAIL_FIRST_SHIP_DTE,
PLANARRIV.COVDURSCHEDDATE	AS	CVRG_DUR_SCHED_DTE,
PLANARRIV.DELIVERYDATE	AS	DLV_DTE,
PLANARRIV.DEPARTUREDATE	AS	DEPTR_DTE,
PLANARRIV.DEST	AS	DEST_LOC_KEY,
case when PLANARRIV.FIRMPLANSW IS NULL then 'U' when UPPER(PLANARRIV.FIRMPLANSW) in ('YES','TRUE','1') then 'Y'
when UPPER(PLANARRIV.FIRMPLANSW) in ('NO','FALSE','0') then 'N' else 'U' end as FIRM_PLN_FLAG,
PLANARRIV.HEADERSEQNUM	AS	HDR_SEQ_NBR,
PLANARRIV.ITEM	AS	PROD_KEY,
PLANARRIV.NEEDARRIVDATE AS	REQ_ARV_DTE,
PLANARRIV.NEEDSHIPDATE	AS	REQ_SHIP_DTE,
to_number(PLANARRIV.NONIGNORABLEQTY,28,10) AS NON_IGNR_QTY,
PLANARRIV.ORDERPLACEDATE	AS	ORD_PLACE_DTE,
PLANARRIV.PROJECT	AS	PROJ_NBR,
to_number(PLANARRIV.QTY,28,10) AS SHIP_QTY,
PLANARRIV.REVISEDDATE	AS	REVISED_SHIP_DTE,
PLANARRIV.SCHEDARRIVDATE	AS	SCHED_ARRVL_DTE,
PLANARRIV.SCHEDSHIPDATE	AS	SCHED_SHIP_DTE,
PLANARRIV.SEQNUM	AS	SEQ_NBR,
PLANARRIV.SOURCE  	AS	SRC,
PLANARRIV.SOURCING	AS	SOURCING_MTHD,
to_number(PLANARRIV.SUBSTQTY,28,10) AS SUBST_QTY,
PLANARRIV.TRANSMODE	AS	TRNSPRT_MODE,
PLANARRIV.TRANSNAME	AS	FRT_CARRIER_NAME
FROM {{source('BLUEYONDER','PLANARRIV')}} PLANARRIV