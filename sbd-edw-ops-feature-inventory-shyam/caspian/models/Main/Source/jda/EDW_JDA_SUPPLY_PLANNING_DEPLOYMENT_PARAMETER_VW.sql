SELECT
UPPER(concat(COALESCE(SKUDEPLOYMENTPARAM.ITEM,''),'~',COALESCE(SKUDEPLOYMENTPARAM.LOC,''))) AS SUPPLY_PLNG_DPLY_PARM_KEY,
'JDA' AS {{var('column_srcsyskey')}},
MD5(UPPER(concat(COALESCE(SKUDEPLOYMENTPARAM.ITEM,''),'~',COALESCE(SKUDEPLOYMENTPARAM.LOC,'')))) AS {{var('column_redhashkey')}},
{{var('default_n')}} AS {{var('column_delfromsrcflag')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
SKUDEPLOYMENTPARAM.LOADDTS AS {{var('column_z3loddtm')}},
SKUDEPLOYMENTPARAM.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
SKUDEPLOYMENTPARAM.ALLOCSTRATID AS ALLOC_STRAT_NBR,
CASE WHEN SKUDEPLOYMENTPARAM.CONSTRRECSHIPSW = '1' THEN 'Y' ELSE 'N' END AS CONSTRAINT_RECOMMENDED_SHIP_FLAG,
SKUDEPLOYMENTPARAM.CONSTRUNMETDMDDISPLAYDUR AS CONSTRAINT_UNMET_DMD_DY_CNT,
SKUDEPLOYMENTPARAM.DEPLOYDETAILLEVEL AS DEPLOY_DTL_LVL,
SKUDEPLOYMENTPARAM.DYNDEPLDUR AS DYN_DPLY_DUR_DY_CNT,
SKUDEPLOYMENTPARAM.HOLDBACKQTY AS HOLD_BK_QTY,
SKUDEPLOYMENTPARAM.INCSTKOUTCOST AS INCREMENTAL_STOCKOUT_COST,
SKUDEPLOYMENTPARAM.INITSTKOUTCOST AS INIT_STOCKOUT_COST,
case when length(trim(UPPER((SKUDEPLOYMENTPARAM.ITEM)))) <1 then {{var('default_mapkey')}} when UPPER(SKUDEPLOYMENTPARAM.ITEM)
IS NULL then {{var('default_mapkey')}} else UPPER(SKUDEPLOYMENTPARAM.ITEM) end AS PROD_KEY,
case when length(trim(UPPER((SKUDEPLOYMENTPARAM.LOC)))) <1 then {{var('default_mapkey')}} when UPPER(SKUDEPLOYMENTPARAM.LOC)
IS NULL then {{var('default_mapkey')}} else UPPER(SKUDEPLOYMENTPARAM.LOC) end AS LOC_KEY,
SKUDEPLOYMENTPARAM.LOCPRIORITY AS LOC_PRYRT_NBR,
SKUDEPLOYMENTPARAM.MAXBUCKETDUR AS MAX_BUCKET_DUR,
SKUDEPLOYMENTPARAM.MINALLOCDUR AS MIN_ALLOC_DUR,
SKUDEPLOYMENTPARAM.PUSHOPT AS PUSH_OPT,
SKUDEPLOYMENTPARAM.RECSHIPCAL AS SHIP_CLNDR_TYP,
SKUDEPLOYMENTPARAM.RECSHIPDUR AS SHIP_DUR,
SKUDEPLOYMENTPARAM.RECSHIPPUSHOPT AS SHIP_PUSH_OPT,
SKUDEPLOYMENTPARAM.RECSHIPSUPPLYRULE AS SHIP_SUPPLY_RULE,
SKUDEPLOYMENTPARAM.RSALLOCRULE AS SHIP_ALLOC_RULE,
SKUDEPLOYMENTPARAM.SECRSALLOCRULE AS SCNDRY_SHIP_ALLOC_RULE,
SKUDEPLOYMENTPARAM.SHORTAGEDUR AS SHORT_DUR_DY_CNT,
SKUDEPLOYMENTPARAM.SHORTAGESSFACTOR AS SHORT_SFTY_STK_FACTOR_VAL,
SKUDEPLOYMENTPARAM.SKUPRIORITY AS SKU_PRYRT_NBR,
SKUDEPLOYMENTPARAM.SLOWMOVERRELEASETHRESHOLD AS SLOW_MOVER_RELS_THRESHOLD_VAL,
SKUDEPLOYMENTPARAM.SOURCESSRULE AS SRC_SFTY_STK_RULE,
SKUDEPLOYMENTPARAM.STOCKAVAILDUR AS STK_AVAIL_DUR,
SKUDEPLOYMENTPARAM.SURPLUSRESTOCKCOST AS SURPLUS_RESTOCK_COST,
SKUDEPLOYMENTPARAM.SURPLUSSSFACTOR AS SURPLUS_SFTY_STK_FACTOR_VAL,
SKUDEPLOYMENTPARAM.U_RECSHIPPUSHOPTOVERRIDE AS LOCK_TYP_OVRD,
SKUDEPLOYMENTPARAM.UNITSTOCKLOWCOST AS UNIT_STK_LOW_COST,
SKUDEPLOYMENTPARAM.UNITSTOCKOUTCOST AS UNIT_STK_OUT_COST
FROM {{source('JDA','SKUDEPLOYMENTPARAM')}}  SKUDEPLOYMENTPARAM