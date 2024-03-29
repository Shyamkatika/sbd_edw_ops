Select
UPPER(CONCAT(coalesce(SOURCING.ITEM::varchar,''),'~',coalesce(SOURCING.DEST::varchar,''),'~' ,coalesce(SOURCING.SOURCE::varchar,''),'~' ,coalesce(SOURCING.SOURCING::varchar,''))) AS SUPPLY_PLNG_PROD_SOURCING_KEY,
'JDAEDW' AS {{var('column_srcsyskey')}},
MD5(UPPER(CONCAT(coalesce(SOURCING.ITEM::varchar,''),'~',coalesce(SOURCING.DEST::varchar,''),'~' ,coalesce(SOURCING.SOURCE::varchar,''),'~' ,coalesce(SOURCING.SOURCING::varchar,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
SOURCING.LOADDTS AS {{var('column_z3loddtm')}},
SOURCING.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
TO_TIMESTAMP(SOURCING.DISC) as DISCONTINUATION_DTE,
TO_TIMESTAMP(SOURCING.EFF) as PLAN_EFF_START_DTE,
TO_TIMESTAMP(SOURCING.NONEWSUPPLYDATE) as NO_NEW_SUPPLY_DTE,
SOURCING.EXPEDITECOST as TRANSIT_EXPEDITE_COST,
SOURCING.SOURCINGCOST as UNIT_SOURCING_COST,
SOURCING.PURCHCOST as PUR_COST,
SOURCING.TRANSCOST as TRNSPRT_COST,
SOURCING.FACTOR as FACTOR,
SOURCING.ORDERFULFILLPENALTYFACTOR as ORD_FULFILL_PENALTY_FACTOR,
SOURCING.ROUNDINGFACTOR as RND_FACTOR,
SOURCING.SHRINKAGEFACTOR as SHRINKAGE_FACTOR,
SOURCING.YIELDFACTOR as YIELD_FACTOR_VAL,
SOURCING.LOADDUR as LOD_DUR,
SOURCING.REPLENDUR as REPLENISH_DUR,
SOURCING.UNLOADDUR as UNLD_DUR,
SOURCING.MAJORSHIPQTY as MIN_SHIP_QTY,
SOURCING.MINORSHIPQTY as INCREMENTAL_SHIP_QTY,
SOURCING.SPLITQTY as MIN_SPLIT_QTY,
SOURCING.MAXSHIPQTY as MAX_SHIP_QTY,
SOURCING.PRIORITY as SOURCING_PRYRT_NBR,
SOURCING.MAXLEADTIME as MAX_LEADTIME_DUR,
SOURCING.MINLEADTIME as MIN_LEADTIME_DUR,
SOURCING.SUPPLYLEADTIME as SUPPLY_LEAD_TIMEDURATION,
SOURCING.TOTLEADTIMESD as TOT_LEADTIME_DUR,
CASE WHEN SOURCING.U_AUTOAPPROVE = '1' THEN 'Y'  WHEN SOURCING.U_AUTOAPPROVE = '0' THEN 'N' ELSE {{var('default_flag')}} END as AUTO_APRV_FLAG,
CASE WHEN SOURCING.LOTSIZESENABLEDSW = '1' THEN 'Y' WHEN SOURCING.LOTSIZESENABLEDSW = '0' THEN 'N' ELSE {{var('default_flag')}} END as LOT_SZ_ENABLED_FLAG,
SOURCING.ARRIVCAL AS ARRVL_CLNDR_ID,
case when length(trim(UPPER((SOURCING.DEST)))) <1 then {{var('default_mapkey')}} when UPPER(SOURCING.DEST) IS NULL then {{var('default_mapkey')}} else UPPER(SOURCING.DEST) end AS DEST_LOC_KEY,
case when length(trim(UPPER((SOURCING.ITEM)))) <1 then {{var('default_mapkey')}} when UPPER(SOURCING.ITEM) IS NULL then {{var('default_mapkey')}} else UPPER(SOURCING.ITEM) end AS PROD_KEY,
SOURCING.REVIEWCAL  AS RVW_CLNDR_ID,
SOURCING.SHIPCAL AS SHIP_CLNDR_ID,
case when length(trim(UPPER((SOURCING.SOURCE)))) <1 then {{var('default_mapkey')}} when UPPER(SOURCING.SOURCE) IS NULL then {{var('default_mapkey')}} else UPPER(SOURCING.SOURCE) end AS SRC_LOC_KEY,
SOURCING.ORDERGROUP AS ORD_GRP,
SOURCING.SOURCING AS SOURCING_TYP,
SOURCING.TRANSMODE AS TRNSPRT_MODE,
SOURCING.CONVENIENTADJUPPCT as NEXT_CONVENIENT_SHIP_QTY, 
SOURCING.CONVENIENTOVERRIDETHRESHOLD as THRESHOLD_CONVENIENT_SHIP_QTY, 
CASE WHEN SOURCING.ENABLESW = '1' THEN 'Y'  WHEN SOURCING.ENABLESW = '0' THEN 'N' ELSE {{var('default_flag')}} END as OPTIMIZATION_OPTIMIZER_FLAG, 
SOURCING.PULLFORWARDDUR as PULL_FORWARD_DUR, 
CASE WHEN SOURCING.USELOOKAHEADSW = '1' THEN 'Y' WHEN SOURCING.USELOOKAHEADSW = '0' THEN 'N'  ELSE {{var('default_flag')}} END as USE_LOOK_AHEAD_FLAG, 
/*SOURCING.ROUNDINGFACTOR as ORD_OPTIMIZATION_RND_FACTOR, 
SOURCING.TOTLEADTIMESD as STD_DEVN_TOT_LEADTIME,
SOURCING.ORDERGROUP as SKU_ORD_OPTIMIZATION_GRP,*/
SOURCING.CONVENIENTADJDOWNPCT as EARLIER_CONVENIENT_SHIP_QTY
FROM {{source('JDAEDW','SOURCING')}} SOURCING
