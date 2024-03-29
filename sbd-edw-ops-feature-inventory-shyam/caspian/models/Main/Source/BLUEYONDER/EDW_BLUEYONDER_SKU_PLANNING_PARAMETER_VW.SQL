SELECT
UPPER(CONCAT(COALESCE(SKUPLANNINGPARAM.ITEM::VARCHAR,''),'~',COALESCE(SKUPLANNINGPARAM.LOC::VARCHAR,''))) AS SKU_PLNG_PARM_KEY,
'BLUEYONDER' AS  {{var('column_srcsyskey')}},  
MD5(UPPER(CONCAT(COALESCE(SKUPLANNINGPARAM.ITEM::VARCHAR,''),'~',COALESCE(SKUPLANNINGPARAM.LOC::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
SKUPLANNINGPARAM.LOADDTS AS  {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
SKUPLANNINGPARAM.LOADDTS AS {{var('column_z3loddtm')}},
SKUPLANNINGPARAM.AGGEXCESSSUPPLYRULE    AS    AGRT_EXCESS_SUPPLY_RULE,
SKUPLANNINGPARAM.AGGSKURULE    AS    AGRT_SKU_RULE,
SKUPLANNINGPARAM.AGGUNDERSUPPLYRULE    AS    AGRT_UNDER_SUPPLY_RULE,
case when SKUPLANNINGPARAM.ALLOCBATCHSW IS NULL then 'U' when UPPER(SKUPLANNINGPARAM.ALLOCBATCHSW) in ('YES','TRUE','1') then 'Y'
when UPPER(SKUPLANNINGPARAM.ALLOCBATCHSW) in ('NO','FALSE','0') then 'N' else 'U' end as ALLOC_BATCH_FLAG,
SKUPLANNINGPARAM.ATPDUR    AS    AVAIL_PROMISE_DUR_DY_CNT,
SKUPLANNINGPARAM.ATPRULE    AS    AVAIL_PROMISE_RULE,
SKUPLANNINGPARAM.BUFFERLEADTIME    AS    BUFFER_LEADTIME_DY_CNT,
SKUPLANNINGPARAM.CMPFIRMDUR    AS    CMP_FIRM_DUR,
SKUPLANNINGPARAM.CONSTRINVOPT    AS    CONSTRAINT_INVTY_OPT,
SKUPLANNINGPARAM.COSTUOM    AS    UOM_COST,    
case when SKUPLANNINGPARAM.CPPLOCKSW IS NULL then 'U' when UPPER(SKUPLANNINGPARAM.CPPLOCKSW) in ('YES','TRUE','1') then 'Y'
when UPPER(SKUPLANNINGPARAM.CPPLOCKSW) in ('NO','FALSE','0') then 'N' else 'U' end as CONSTRAINT_PRODTN_PLNG_LOCK_FLAG,
SKUPLANNINGPARAM.CPPPRIORITY    AS    CONSTRAINT_PRODTN_PLNG_PRYRT_NBR,
SKUPLANNINGPARAM.CUMLEADTIMEADJDUR    AS    CUML_LEADTIME_ADJ_DUR,
SKUPLANNINGPARAM.CUMLEADTIMEDUR    AS    CUML_LEADTIME_DUR,
SKUPLANNINGPARAM.CUMLEADTIMERULE    AS    CUML_LEADTIME_RULE,
SKUPLANNINGPARAM.DEPDMDOPT    AS    DPDNT_DMD_OPT,
SKUPLANNINGPARAM.DRPCOVDUR    AS    PLNG_CVRG_DUR,
SKUPLANNINGPARAM.DRPFRZDUR    AS    PLNG_FREEZE_DUR,
SKUPLANNINGPARAM.DRPRULE    AS    PLNG_RULE,
SKUPLANNINGPARAM.DRPTIMEFENCEDATE    AS    PLNG_TM_FENCE_END_DTE,
SKUPLANNINGPARAM.DRPTIMEFENCEDUR    AS    PLNG_TM_FENCE_DUR,
SKUPLANNINGPARAM.EOQ    AS    ECONOMIC_ORD_QTY,
SKUPLANNINGPARAM.EXPDATE    AS    PLNG_EXPR_DTE,
case when SKUPLANNINGPARAM.EXTERNALSKUSW IS NULL then 'U' when UPPER(SKUPLANNINGPARAM.EXTERNALSKUSW) in ('YES','TRUE','1') then 'Y'
when UPPER(SKUPLANNINGPARAM.EXTERNALSKUSW) in ('NO','FALSE','0') then 'N' else 'U' end as EXTNL_SKU_FLAG,
SKUPLANNINGPARAM.FIRSTREPLENDATE    AS    FIRST_REPL_DTE,
SKUPLANNINGPARAM.FWDBUYMAXDUR    AS    FORWARD_BUY_MAX_DUR,
SKUPLANNINGPARAM.HOLDINGCOST    AS    UNIT_HOLDING_COST_PRC,
SKUPLANNINGPARAM.INCDRPQTY    AS    INCREMENTAL_DRP_PLNG_QTY,
SKUPLANNINGPARAM.INCMPSQTY    AS    INCREMENTAL_MPS_PLNG_QTY,
SKUPLANNINGPARAM.INHANDLINGCOST    AS    RCV_HDLNG_COST_PRC,
SKUPLANNINGPARAM.ITEM    AS    PROD_KEY,
SKUPLANNINGPARAM.LASTFRZSTART    AS    LAST_FREEZE_START_DTE,
SKUPLANNINGPARAM.LASTPLANSTART    AS    LAST_PLAN_START_DTE,
SKUPLANNINGPARAM.LIMITPLANARRIVPUBLISHDUR    AS    PLND_ARRVL_RCRD_DUR,
case when SKUPLANNINGPARAM.LIMITPLANARRIVPUBLISHSW IS NULL then 'U' when UPPER(SKUPLANNINGPARAM.LIMITPLANARRIVPUBLISHSW) in ('YES','TRUE','1') then 'Y'
when UPPER(SKUPLANNINGPARAM.LIMITPLANARRIVPUBLISHSW) in ('NO','FALSE','0') then 'N' else 'U' end as PLND_ARRVL_RCRD_PUBLISH_FLAG,
SKUPLANNINGPARAM.LOC    AS    LOC_KEY,
SKUPLANNINGPARAM.LOOKAHEADDUR    AS    LOOK_AHEAD_DUR,
SKUPLANNINGPARAM.MAXCOVDUR    AS    MAX_CVRG_DUR,
SKUPLANNINGPARAM.MAXOH    AS    MAX_ON_HAND_QTY,
SKUPLANNINGPARAM.MAXOHRULE    AS    MAX_ON_HAND_RULE,
SKUPLANNINGPARAM.MFGFRZDUR    AS    MFG_FREEZE_DUR,
SKUPLANNINGPARAM.MFGLEADTIME    AS    MFG_LEADTIME_DY_CNT,
SKUPLANNINGPARAM.MINDRPQTY    AS    MIN_DRP_PLNG_QTY,
SKUPLANNINGPARAM.MINMPSQTY    AS    MIN_MPS_PLNG_QTY,
SKUPLANNINGPARAM.MPSCOVDUR    AS    MPS_COVERED_DUR,
SKUPLANNINGPARAM.MPSRULE    AS    MPS_PLNG_RULE,
SKUPLANNINGPARAM.MPSTIMEFENCEDATE    AS    MPS_TM_FENCE_END_DTE,
SKUPLANNINGPARAM.MPSTIMEFENCEDUR    AS    MPS_TM_FENCE_DUR,
SKUPLANNINGPARAM.ORDERINGCOST    AS    ORD_FIXED_COST_PRC,
SKUPLANNINGPARAM.ORDERPOINTMINDUR    AS    ORD_POINT_MIN_DUR,
SKUPLANNINGPARAM.ORDERPOINTMINQTY    AS    ORD_POINT_MIN_QTY,
SKUPLANNINGPARAM.ORDERPOINTMINRULE    AS    ORD_POINT_MIN_RULE,
SKUPLANNINGPARAM.ORDERPOINTRULE    AS    ORD_POINT_RULE,
case when SKUPLANNINGPARAM.ORDERSKUDETAILSW IS NULL then 'U' when UPPER(SKUPLANNINGPARAM.ORDERSKUDETAILSW) in ('YES','TRUE','1') then 'Y'
when UPPER(SKUPLANNINGPARAM.ORDERSKUDETAILSW) in ('NO','FALSE','0') then 'N' else 'U' end as ORD_SKU_DTL_POPULATE_FLAG,
SKUPLANNINGPARAM.ORDERUPTOLEVELMAXDUR    AS    ORD_LVL_MAX_DUR,
SKUPLANNINGPARAM.ORDERUPTOLEVELMAXQTY    AS    ORD_LVL_MAX_QTY,
SKUPLANNINGPARAM.ORDERUPTOLEVELMAXRULE    AS    ORD_LVL_MAX_RULE,
SKUPLANNINGPARAM.OUTHANDLINGCOST    AS    SHIP_HDLNG_COST,
case when SKUPLANNINGPARAM.PEGSW IS NULL then 'U' when UPPER(SKUPLANNINGPARAM.PEGSW) in ('YES','TRUE','1') then 'Y'
when UPPER(SKUPLANNINGPARAM.PEGSW) in ('NO','FALSE','0') then 'N' else 'U' end as SUPPLY_TO_DMD_PEG_FLAG,
SKUPLANNINGPARAM.PLANDUR    AS    PLAN_DUR,
SKUPLANNINGPARAM.PLANLEADTIME    AS    PLAN_LEADTIME_DY_CNT    ,
SKUPLANNINGPARAM.PLANLEADTIMERULE    AS    PLAN_LEADTIME_RULE,
SKUPLANNINGPARAM.PLANSHIPFRZDUR    AS    PLAN_SHIP_FREEZE_DUR,
SKUPLANNINGPARAM.PRODCAL    AS    PRODTN_CLNDR_ID,
SKUPLANNINGPARAM.PRODSTARTDATE    AS    PRODTN_START_DTE,
SKUPLANNINGPARAM.PRODSTOPDATE    AS    PRODTN_STOP_DTE,
SKUPLANNINGPARAM.RECSCHEDRCPTSDUR    AS    RECOMMENDED_SCHED_RCPT_DUR,
SKUPLANNINGPARAM.RESTRICTDUR    AS    RESTRCT_PLAN_DUR,
SKUPLANNINGPARAM.ROUNDINGFACTOR    AS    PLND_ORD_RND_FACTOR_VAL,
SKUPLANNINGPARAM.SHRINKAGEFACTOR    AS    SHRINKAGE_FACTOR_VAL,
case when SKUPLANNINGPARAM.SKULOTSIZEENABLESW IS NULL then 'U' when UPPER(SKUPLANNINGPARAM.SKULOTSIZEENABLESW) in ('YES','TRUE','1') then 'Y'
when UPPER(SKUPLANNINGPARAM.SKULOTSIZEENABLESW) in ('NO','FALSE','0') then 'N' else 'U' end as SKU_LOT_SZ_ENABLE_FLAG,
SKUPLANNINGPARAM.SUPPINVCAL    AS    SUPPLY_INVTY_CLNDR_ID,
SKUPLANNINGPARAM.SUPSDMINDMDCOVDUR    AS    MIN_DMD_DUR,
SKUPLANNINGPARAM.WORKINGCAL    AS    WORKING_CLNDR_ID    
FROM {{source('BLUEYONDER','SKUPLANNINGPARAM')}} SKUPLANNINGPARAM
