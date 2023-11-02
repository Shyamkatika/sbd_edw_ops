select
UPPER(CONCAT(coalesce(NETWORK.SOURCE,''),'~',coalesce(NETWORK.TRANSMODE,''),'~' ,coalesce(NETWORK.DEST,''))) AS NETWORK_KEY,
'JDAEDW' AS {{var('column_srcsyskey')}},
MD5(UPPER(CONCAT(coalesce(NETWORK.SOURCE,''),'~',coalesce(NETWORK.TRANSMODE,''),'~' ,coalesce(NETWORK.DEST,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
NETWORK.LOADDTS AS {{var('column_z3loddtm')}},
NETWORK.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
NETWORK.ARRIVCAL AS ARRVL_CLNDR_ID,
case when UPPER(NETWORK.AUTOAPPROVALSW) in ('NO','FALSE','0') or UPPER(NETWORK.AUTOAPPROVALSW) is null
or length(TRIM(NETWORK.AUTOAPPROVALSW))<1 then 'N' when UPPER(NETWORK.AUTOAPPROVALSW) in ('YES','TRUE','1')
then 'Y' else 'U' end as ORD_APRV_FLAG,
NETWORK.CARRIER AS TRNSPRT_CARRIER_NAME,
NETWORK.COVDURADJTOLERANCE AS COVER_DUR_ADJUST_TLRNCE,
case when length(trim(UPPER((NETWORK.DEST)))) <1 then {{var('default_mapkey')}} when UPPER(NETWORK.DEST)
IS NULL then {{var('default_mapkey')}} else UPPER(NETWORK.DEST) end AS DEST_LOC_KEY,
NETWORK.DISTANCE AS LOC_DISTANCE,
NETWORK.DISTANCEUOM AS UOM_DISTANCE_UNIT,
NETWORK.FWDBUYACTIVEDUR AS DEAL_DSCTU_DTE_CNT,
NETWORK.LANEGROUPID AS LANE_GRP_NBR,
NETWORK.LOADBUILDADJDOWNTOLERANCE AS ADJUSTING_LOD_DECR_PCT,
NETWORK.LOADBUILDADJUPTOLERANCE AS ADJUSTING_LOD_INCREASE_PCT,
NETWORK.LOADBUILDRULE AS LOD_BUILD_RULE_TYP,
NETWORK.LOADMINIMUMRULE AS LOD_MIN_RULE_TYP,
NETWORK.LOADSEQRULE AS LOD_SEQ_RULE_TYP,
NETWORK.LOADTIME AS VHCL_LOD,
NETWORK.LOADTOLERANCE AS LOD_TLRNCE_PCT,
NETWORK.MUSTGODAYS AS BUILD_TRNSPRT_LOD_DY_CNT,
NETWORK.ORDERREVIEWCAL AS ORD_RVW_CLNDR,
NETWORK.PRIMARYLSDUR AS PRIM_LOD_SATISFACTION_DUR,
NETWORK.PRIMARYLSRULE AS PRIM_LOD_SATISFACTION_RULE_TYP,
NETWORK.RANK AS TRNSPRT_MODE_RANK_NBR,
NETWORK.RATEPERCWT AS SHIP_COST_VAL,
NETWORK.SECONDARYLSRULE AS SCNDRY_LOD_SATISFACTION_RULE_TYP,
NETWORK.SHIPCAL AS SHIP_CLNDR,
NETWORK.SOURCE AS SOURCING_MTHD_TYP,
NETWORK.TRANSCAL AS SRC_TRNSPRT_DEPART_DTE,
NETWORK.TRANSLEADTIME AS TOT_TRANSIT,
NETWORK.TRANSLEADTIMESD AS TRANSIT_LEADTIME_STD_DEVN,
NETWORK.TRANSMODE AS TRNSPRT_MODE_NAME,
NETWORK.TRANSMODEMINRULE AS TRNSPRT_MIN_RULE,
NETWORK.UNLOADTIME AS VHCL_UNLD,
NETWORK.VENDORMINRULE AS VEND_MIN_RULE,
case when UPPER(NETWORK.U_DEFAULT) in ('NO','FALSE','0') or UPPER(NETWORK.U_DEFAULT) is null
or length(TRIM(NETWORK.U_DEFAULT))<1 then 'N' when UPPER(NETWORK.U_DEFAULT) in ('YES','TRUE','1')
then 'Y' else 'U' end as DFLT_SOURCING_LANE_FLAG,
NETWORK.U_DEPRECCYCLE AS SAP_DPLY_RECOMMENDATION,
case when UPPER(NETWORK.U_INCLUDE_INTRANSITS_YN) in ('NO','FALSE','0') or UPPER(NETWORK.U_INCLUDE_INTRANSITS_YN) is null
or length(TRIM(NETWORK.U_INCLUDE_INTRANSITS_YN))<1 then 'N' when UPPER(NETWORK.U_INCLUDE_INTRANSITS_YN) in ('YES','TRUE','1')
then 'Y' else 'U' end as INCLD_IN_TRANSIT_FLAG,
--NETWORK.U_RECSHIPDAYS AS RECOMMENDED_SHIP,
NETWORK.U_RECSHIPFIRMHRZN AS RECOMMENDED_SHIP_FIRM_HORIZON,
NETWORK.U_VENDOR AS VEND_NAME,
case when UPPER(NETWORK.USEALTTRANSMODESW) in ('NO','FALSE','0') or UPPER(NETWORK.USEALTTRANSMODESW) is null
or length(TRIM(NETWORK.USEALTTRANSMODESW))<1 then 'N' when UPPER(NETWORK.USEALTTRANSMODESW) in ('YES','TRUE','1')
then 'Y' else 'U' end as ALT_TRNSPRT_FLAG,
NETWORK.FWDBUYEFFECTPCT AS FORWARD_BUY_EFF_PCT,
NETWORK.LOOKAHEADDAYS AS LOOK_AHEAD_DYS,
NETWORK.U_DESTCTRY AS DEST_CNTRY_CD,
NETWORK.U_DESTTRANSZONE as DEST_TRNSPRT_ZONE,
CASE WHEN NETWORK.U_LTV_CAP_SW = '1' THEN 'Y' WHEN NETWORK.U_LTV_CAP_SW = '0' THEN 'N'  ELSE {{var('default_flag')}} END as LEADTIME_CAP_FLAG, 
NETWORK.U_PICKPACKTIME as PICK_PKG_TM,
NETWORK.U_SAPROUTE as SAP_ROUTE,
NETWORK.U_SAPROUTELT as SAP_ROUTE_LEADTIME,
NETWORK.U_SHIPCOND as SAP_SHIP_COND,
NETWORK.U_SHIPPOINT as SHIP_POINT,
NETWORK.U_SHIPTO as SHIPTO_CD,
NETWORK.U_SOLDTO as SOLDTO_CD,
NETWORK.U_SRCCTRY AS SRC_CNTRY_CD,
NETWORK.U_SRCTRANSZONE as SRC_TRNSPRT_ZONE,
NETWORK.ORDERCOST as BUILD_TRNSPRT_LOD_COST,
NETWORK.RIOMAXCOVDUR as REPL_MAX_CVRG_DUR,
NETWORK.RIOMINCOVDUR as REPL_MIN_CVRG_DUR,
NETWORK.U_RECSHIPDAYS as RECOMMENDED_SHIP_DTE
FROM {{source('JDAEDW','NETWORK')}} NETWORK