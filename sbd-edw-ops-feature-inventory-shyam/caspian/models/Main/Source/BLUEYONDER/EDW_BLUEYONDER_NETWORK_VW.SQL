SELECT
UPPER(CONCAT(COALESCE(NETWORK.SOURCE::VARCHAR,''),'~',COALESCE(NETWORK.TRANSMODE::VARCHAR,''),'~',COALESCE(NETWORK.DEST::VARCHAR,''))) AS NETWORK_KEY,
'BLUEYONDER' AS  {{var('column_srcsyskey')}},  
MD5(UPPER(CONCAT(COALESCE(NETWORK.SOURCE::VARCHAR,''),'~',COALESCE(NETWORK.TRANSMODE::VARCHAR,''),'~',COALESCE(NETWORK.DEST::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
NETWORK.LOADDTS AS  {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
NETWORK.LOADDTS AS {{var('column_z3loddtm')}},
NETWORK.ARRIVCAL    AS  ARRVL_CLNDR_ID,
case when NETWORK.AUTOAPPROVALSW IS NULL then 'U' when UPPER(NETWORK.AUTOAPPROVALSW) in ('YES','TRUE','1') then 'Y'
when UPPER(NETWORK.AUTOAPPROVALSW) in ('NO','FALSE','0') then 'N' else 'U' end as ORD_APRV_FLAG,
NETWORK.CARRIER AS  TRNSPRT_CARRIER_NAME,
NETWORK.COVDURADJTOLERANCE  AS  COVER_DUR_ADJUST_TLRNCE,
NETWORK.DEST    AS  DEST_LOC_KEY,
NETWORK.DISTANCE    AS  LOC_DISTANCE,
NETWORK.DISTANCEUOM AS  UOM_DISTANCE_UNIT,
NETWORK.FWDBUYACTIVEDUR AS  DEAL_DSCTU_DTE_CNT,
NETWORK.LANEGROUPID AS  LANE_GRP_NBR,
NETWORK.LOADBUILDADJDOWNTOLERANCE   AS  ADJUSTING_LOD_DECR_PCT,
NETWORK.LOADBUILDADJUPTOLERANCE AS  ADJUSTING_LOD_INCREASE_PCT,
NETWORK.LOADBUILDRULE   AS  LOD_BUILD_RULE_TYP,
NETWORK.LOADMINIMUMRULE AS  LOD_MIN_RULE_TYP,
NETWORK.LOADSEQRULE AS  LOD_SEQ_RULE_TYP,
NETWORK.LOADTIME    AS  VHCL_LOD,
NETWORK.LOADTOLERANCE   AS  LOD_TLRNCE_PCT,
NETWORK.MUSTGODAYS  AS  BUILD_TRNSPRT_LOD_DY_CNT,
NETWORK.ORDERREVIEWCAL  AS  ORD_RVW_CLNDR,
NETWORK.PRIMARYLSDUR    AS  PRIM_LOD_SATISFACTION_DUR,
NETWORK.PRIMARYLSRULE   AS  PRIM_LOD_SATISFACTION_RULE_TYP,
NETWORK.RANK    AS  TRNSPRT_MODE_RANK_NBR,
NETWORK.RATEPERCWT  AS  SHIP_COST_VAL,
NETWORK.SECONDARYLSRULE AS  SCNDRY_LOD_SATISFACTION_RULE_TYP,
NETWORK.SHIPCAL AS  SHIP_CLNDR,
NETWORK.SOURCE  AS  SOURCING_MTHD_TYP,
NETWORK.TRANSCAL    AS  SRC_TRNSPRT_DEPART_DTE,
NETWORK.TRANSLEADTIME   AS  TOT_TRANSIT,
NETWORK.TRANSLEADTIMESD AS  TRANSIT_LEADTIME_STD_DEVN,
NETWORK.TRANSMODE   AS  TRNSPRT_MODE_NAME,
NETWORK.TRANSMODEMINRULE    AS  TRNSPRT_MIN_RULE,
NETWORK.UNLOADTIME  AS  VHCL_UNLD,
NETWORK.VENDORMINRULE   AS  VEND_MIN_RULE,
case when NETWORK.USEALTTRANSMODESW IS NULL then 'U' when UPPER(NETWORK.USEALTTRANSMODESW) in ('YES','TRUE','1') then 'Y'
when UPPER(NETWORK.USEALTTRANSMODESW) in ('NO','FALSE','0') then 'N' else 'U' end as ALT_TRNSPRT_FLAG,
NETWORK.ORDERCOST   AS  BUILD_TRNSPRT_LOD_COST,
NETWORK.RIOMAXCOVDUR    AS  REPL_MAX_CVRG_DUR,
NETWORK.RIOMINCOVDUR    AS  REPL_MIN_CVRG_DUR,
NETWORK.FWDBUYEFFECTPCT AS  FORWARD_BUY_EFF_PCT,
NETWORK.LOOKAHEADDAYS   AS  LOOK_AHEAD_DYS
FROM {{source('BLUEYONDER','NETWORK')}} NETWORK