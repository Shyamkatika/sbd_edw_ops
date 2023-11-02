SELECT 
UPPER(CONCAT(coalesce(SKUDEMANDPARAM.ITEM::VARCHAR,''),'~',coalesce(SKUDEMANDPARAM.LOC::VARCHAR,''))) AS SUPPLY_PLNG_DMD_PARM_KEY,
'JDA' AS {{var('column_srcsyskey')}},
MD5(UPPER(CONCAT(coalesce(SKUDEMANDPARAM.ITEM::VARCHAR,''),'~',coalesce(SKUDEMANDPARAM.LOC::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
SKUDEMANDPARAM.LOADDTS AS {{var('column_z3loddtm')}},
SKUDEMANDPARAM.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
CASE WHEN SKUDEMANDPARAM.ADJINDDMDNEEDDATEEARLYSW = '1' THEN 'Y' WHEN SKUDEMANDPARAM.ADJINDDMDNEEDDATEEARLYSW = '0' THEN 'N'  ELSE  {{var('default_flag')}}  END AS ADJ_IDPDNT_DMD_FLAG,
SKUDEMANDPARAM.ALLOCCAL AS ALLOC_CLNDR_NAME,
SKUDEMANDPARAM.ALLOCCALGROUP AS ALLOC_CLNDR_GRP_NAME,
SKUDEMANDPARAM.ANNUALFCST AS ANNL_FCST_AMT,
CASE WHEN SKUDEMANDPARAM.CCPSW = '1' THEN 'Y' WHEN SKUDEMANDPARAM.CCPSW = '0' THEN 'N' ELSE  {{var('default_flag')}} END AS CUST_COMMIT_POINT_FLAG,
TRY_TO_TIMESTAMP_NTZ(replace(SKUDEMANDPARAM.CURRENTPERIODFCSTDATE,'"',''),'YYYY-MM-DD HH24:MI:SS.FF') AS CURR_PERD_FCST_START_DTE,
SKUDEMANDPARAM.CURRENTPERIODFCSTQTY AS CURR_PERD_FCST_QTY,
SKUDEMANDPARAM.CUSTORDERDUR AS CUST_ORD_DUR_DY_CNT,
SKUDEMANDPARAM.CUSTORDERPRIORITY AS CUST_ORD_PRYRT_NBR,
SKUDEMANDPARAM.DMDREDID AS DMD_RED_ID,
SKUDEMANDPARAM.DMDTODATE AS DMD_TO_DTE_AMT,
SKUDEMANDPARAM.FCSTADJRULE  AS FCST_ADJ_RULE,
SKUDEMANDPARAM.FCSTCONSUMPTIONRULE  AS FCST_CNSMPTN_RULE,
SKUDEMANDPARAM.FCSTMEETEARLYDUR AS SRCH_EARLY_DUR,
SKUDEMANDPARAM.FCSTMEETLATEDUR AS SRCH_LATE_DUR,
SKUDEMANDPARAM.FCSTPRIMCONSDUR AS PRIM_CNSMPTN_DUR,
SKUDEMANDPARAM.FCSTPRIORITY AS FCST_PRYRT_NBR,
SKUDEMANDPARAM.FCSTSECCONSDUR AS SCNDRY_CNSMPTN_DUR_MNS_CNT,
SKUDEMANDPARAM.INDDMDUNITCOST AS USER_DEFINED_STD_COST,
SKUDEMANDPARAM.INDDMDUNITMARGIN AS UNIT_MRGN_AMT,
case when length(trim(SKUDEMANDPARAM.ITEM))<1 then {{var('default_mapkey')}} when SKUDEMANDPARAM.ITEM IS NULL then  {{var('default_mapkey')}} else (SKUDEMANDPARAM.ITEM) end AS PROD_KEY,
case when length(trim(SKUDEMANDPARAM.LOC))<1 then {{var('default_mapkey')}} when SKUDEMANDPARAM.LOC IS NULL then  {{var('default_mapkey')}} else (SKUDEMANDPARAM.LOC) end AS LOC_KEY,
SKUDEMANDPARAM.MASTERCAL AS CLNDR_TYP,
SKUDEMANDPARAM.MAXCUSTORDERSYSDUR AS CUST_ORD_DUR,
SKUDEMANDPARAM.PASTDMDTODATE AS PAST_DMD_DTE_AMT,
SKUDEMANDPARAM.PASTFCSTQTY AS PAST_FCST_QTY,
SKUDEMANDPARAM.PRICECAL AS PRC_CLNDR_TYP,
SKUDEMANDPARAM.PRORATEBYTYPESW AS PRORATE_TYP,
CASE WHEN SKUDEMANDPARAM.PRORATESW= '1' THEN 'Y' WHEN SKUDEMANDPARAM.PRORATESW= '0' THEN 'N' ELSE  {{var('default_flag')}} END  AS PRORATE_FLAG,
SKUDEMANDPARAM.PRORATIONDUR AS PRORATE_DUR_DY_CNT,
TRY_TO_TIMESTAMP_NTZ(replace(SKUDEMANDPARAM.PRORATIONSTARTDATE,'"',''),'YYYY-MM-DD HH24:MI:SS.FF') AS PRORATE_START_DTE,
SKUDEMANDPARAM.SLOWMOVERFCSTTHRESHOLD AS SLOW_MOVER_FCST_THRESHOLD_VAL,
SKUDEMANDPARAM.TARGETEARLYDUR AS TGT_EARLY_DUR_DY_CNT,
SKUDEMANDPARAM.UNITCARCOST AS UNIT_CARRYING_COST_AMT,
SKUDEMANDPARAM.UNITPRICE AS UNIT_SELL_PRC,
SKUDEMANDPARAM.WEEKLYAVGHIST AS WKLY_AVG_AMT
FROM {{source('JDA','SKUDEMANDPARAM')}} SKUDEMANDPARAM