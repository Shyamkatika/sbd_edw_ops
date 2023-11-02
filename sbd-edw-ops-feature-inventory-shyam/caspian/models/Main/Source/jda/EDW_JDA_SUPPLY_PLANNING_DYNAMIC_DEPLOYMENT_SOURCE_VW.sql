SELECT
 UPPER(CONCAT(coalesce(DYNDEPSRC.ITEM::VARCHAR,''),'~',coalesce(DYNDEPSRC.SOURCE::VARCHAR,'')
,'~',coalesce(DYNDEPSRC.EFF::VARCHAR,''),'~',coalesce(DYNDEPSRC.TRANSMODE::VARCHAR,'')
,'~',coalesce(DYNDEPSRC.DEST::VARCHAR,'')))		as	SUPPLY_PLNG_DYN_DPLY_SRC_KEY	,
'JDA' AS {{var('column_srcsyskey')}},
MD5(UPPER(CONCAT(coalesce(DYNDEPSRC.ITEM::VARCHAR,''),'~',coalesce(DYNDEPSRC.SOURCE::VARCHAR,'')
,'~',coalesce(DYNDEPSRC.EFF::VARCHAR,''),'~',coalesce(DYNDEPSRC.TRANSMODE::VARCHAR,'')
,'~',coalesce(DYNDEPSRC.DEST::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
DYNDEPSRC.LOADDTS AS {{var('column_z3loddtm')}},
DYNDEPSRC.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
case when length(trim(UPPER((DYNDEPSRC.ITEM)))) <1 then {{var('default_mapkey')}} when UPPER(DYNDEPSRC.ITEM)
IS NULL then {{var('default_mapkey')}} else UPPER(DYNDEPSRC.ITEM) end AS PROD_KEY,

to_timestamp(DYNDEPSRC.EFF)	as	SRC_EFF_DTE	,
DYNDEPSRC.DYNDEPSRCCOST	as	DYN_TRNSPRT_COST	,
DYNDEPSRC.TRANSMODE	as	TRNSPRT_MODE	,
case when length(trim(UPPER((DYNDEPSRC.ARRIVCAL)))) <1 then {{var('default_mapkey')}} when UPPER(DYNDEPSRC.ARRIVCAL)
IS NULL then {{var('default_mapkey')}} else UPPER(DYNDEPSRC.ARRIVCAL) end AS ARRVL_CLNDR_ID,
DYNDEPSRC.ALTSRCPENALTY	as	ADDTN_PENALTY_COST	,
case when length(trim(UPPER((DYNDEPSRC.SHIPCAL)))) <1 then {{var('default_mapkey')}} when UPPER(DYNDEPSRC.SHIPCAL)
IS NULL then {{var('default_mapkey')}} else UPPER(DYNDEPSRC.SHIPCAL) end AS SHIP_CLNDR_ID,
DYNDEPSRC.SOURCING	as	SOURCING_MTHD_TYP	,

(case when DYNDEPSRC.REVIEWCAL=' ' then NULL else TO_TIMESTAMP(DYNDEPSRC.REVIEWCAL, 'YYYY-MM-DD HH24:MI:SS') end) as RVW_CLNDR_DTE,

--DYNDEPSRC.REVIEWCAL	as	RVW_CLNDR_DTE	,

DYNDEPSRC.DISC	as	DISC_DTE	,
DYNDEPSRC.PULLFORWARDDUR	as	PULL_FORWARD_DUR_DY_CNT	,
DYNDEPSRC.LOADDUR	as	LOD_DUR	,
DYNDEPSRC.UNLOADDUR	as	UNLD_DUR	,
DYNDEPSRC.SPLITQTY	as	SPLIT_QTY	,

DYNDEPSRC.USELOOKAHEADSW	as	LOOKAHEAD_DUR_DY_CNT	,
case when(DYNDEPSRC.ENABLEDYNDEPSW) = '1' then 'Y' else 'N' end	as	DYN_DPLY_FLAG,	
---DYNDEPSRC.TRANSLEADTIME	as	TRANSIT_TM	,
--DYNDEPSRC.RATEPERCWT	as	SRC_PRYRT_COST	,
case when length(trim(UPPER((DYNDEPSRC.SOURCE)))) <1 then {{var('default_mapkey')}} when UPPER(DYNDEPSRC.SOURCE)
IS NULL then {{var('default_mapkey')}} else UPPER(DYNDEPSRC.SOURCE) end AS SRC_LOC_KEY,
case when length(trim(UPPER((DYNDEPSRC.DEST)))) <1 then {{var('default_mapkey')}} when UPPER(DYNDEPSRC.DEST)
IS NULL then {{var('default_mapkey')}} else UPPER(DYNDEPSRC.DEST) end AS DEST_LOC_KEY
FROM {{source('JDA','DYNDEPSRC')}} DYNDEPSRC
