SELECT 
'SAPP10' AS {{var('column_srcsyskey')}},
CONCAT(coalesce(RESB.RSNUM::varchar,''),'~',coalesce(RESB.RSPOS::varchar,''),'~',coalesce(RESB.RSART::varchar,'')) AS INVTY_RSRV_DTL_KEY,
MD5(CONCAT(coalesce(RESB.RSNUM::varchar,''),'~',coalesce(RESB.RSPOS::varchar,''),'~',coalesce(RESB.RSART::varchar,''))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
RESB.LOADDTS AS {{var('column_z3loddtm')}},
RESB.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
RESB.ABLAD AS UNLOD_POINT_NBR,
case when length(trim(UPPER((RESB.AUFPS)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.AUFPS)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.AUFPS) end AS SLS_ORD_LN_1_KEY,
case when length(trim(UPPER((RESB.ERFME)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.ERFME)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.ERFME) end AS DFLT_QTY_UOM_KEY,
case when length(trim(UPPER((RESB.FIPEX)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.FIPEX)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.FIPEX) end AS FUNDS_MGMT_COMMIT_KEY,
case when length(trim(UPPER((RESB.FIPOS)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.FIPOS)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.FIPOS) end AS BUD_ASGN_COMMIT_KEY,
case when length(trim(UPPER((RESB.FISTL)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.FISTL)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.FISTL) end AS BUD_ASGN_FUNDS_CNTR_KEY,
RESB.KDEIN AS SLS_DLV_SCHED_LN_NBR,
RESB.LGNUM AS WHSE_LOC_NBR,
case when length(trim(UPPER((RESB.LGORT)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.LGORT)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.LGORT) end AS INVTY_SLOC_KEY,
case when length(trim(UPPER((RESB.LTXSP)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.LTXSP)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.LTXSP) end AS LANG_KEY,
case when length(trim(UPPER((RESB.MATNR)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.MATNR)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.MATNR) end AS PROD_KEY,
case when length(trim(UPPER((RESB.MEINS)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.MEINS)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.MEINS) end AS RESERVED_QTY_UOM_KEY,
case when length(trim(UPPER((RESB.RFORM)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.RFORM)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.RFORM) end AS VARBL_SZ_FORMULA_LKEY,
case when length(trim(UPPER((RESB.SPLKZ)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.SPLKZ)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.SPLKZ) end AS PROD_CMPNT_BATCH_RUN_KEY,
RESB.UMLGO AS RECORDED_SLOC_NBR,
case when length(trim(UPPER((RESB.UMWRK)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.UMWRK)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.UMWRK) end AS RECORDED_PLANT_LOC_KEY,
case when length(trim(UPPER((RESB.VERTI)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.VERTI)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.VERTI) end AS MRP_DIST_LKEY,
case when length(trim(UPPER((RESB.WAERS)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.WAERS)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.WAERS) end AS STORG_WITHDRAWN_VAL_CRNCY_KEY,
RESB.WEMPF AS SHIPTO_CUST_NBR,
case when length(trim(UPPER((RESB.ZUMEI)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.ZUMEI)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.ZUMEI) end AS CUTTING_MEASURES_UOM_KEY,
case when length(trim(UPPER((RESB.ADRNR)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.ADRNR)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.ADRNR) end AS ADDR_CD_KEY,
case when length(trim(UPPER((RESB.ADVCODE)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.ADVCODE)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.ADVCODE) end AS ADVICE_CD_LKEY,
case when length(trim(UPPER((RESB.ALPGR)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.ALPGR)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.ALPGR) end AS ALT_PROD_BOM_KEY,
RESB.ALPRF AS ALT_PROD_RANK_NBR,
case when length(trim(UPPER((RESB.ALPST)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.ALPST)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.ALPST) end AS ALT_PROD_STRAT_LKEY,
case when length(trim(UPPER((RESB.AUFST)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.AUFST)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.AUFST) end AS ORD_LVL_LKEY,
case when length(trim(UPPER((RESB.AUFWG)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.AUFWG)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.AUFWG) end AS PHNTM_ORD_LVL_LKEY,
case when length(trim(UPPER((RESB.BAUGR)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.BAUGR)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.BAUGR) end AS HIGHER_LVL_PROD_KEY,
case when length(trim(UPPER((RESB.BAUST)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.BAUST)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.BAUST) end AS ASSEMBLY_ORD_LVL_LKEY,
case when length(trim(UPPER((RESB.BAUWG)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.BAUWG)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.BAUWG) end AS ASSEMBLY_ORD_PATH_LKEY,
case when length(trim(UPPER((RESB.BDART)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.BDART)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.BDART) end AS RP_TYP_LKEY,
case when length(trim(UPPER((RESB.BEIKZ)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.BEIKZ)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.BEIKZ) end AS PROD_PROVISIONED_FROM_LKEY,
case when length(trim(UPPER((RESB.BERKZ)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.BERKZ)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.BERKZ) end AS PROD_STG_LKEY,
case when length(trim(UPPER((RESB.BWART)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.BWART)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.BWART) end AS MVMNT_TYP_LKEY,
case when length(trim(UPPER((RESB.CH_PROC)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.CH_PROC)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.CH_PROC) end AS PRECEDING_CHG_PROC_LKEY,
case when length(trim(UPPER((RESB.EKGRP)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.EKGRP)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.EKGRP) end AS PUR_GRP_LKEY,
case when length(trim(UPPER((RESB.ERSKZ)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.ERSKZ)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.ERSKZ) end AS SPARE_PART_LKEY,
case when length(trim(UPPER((RESB.FLGAT)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.FLGAT)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.FLGAT) end AS SEQ_CTGY_LKEY,
case when length(trim(UPPER((RESB.FUNCT)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.FUNCT)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.FUNCT) end AS DIST_FUNC_LKEY,
case when length(trim(UPPER((RESB.GEBER)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.GEBER)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.GEBER) end AS BUD_FUND_KEY,
case when length(trim(UPPER((RESB.GRANT_NBR)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.GRANT_NBR)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.GRANT_NBR) end AS GRANT_KEY,
case when length(trim(UPPER((RESB.GSBER)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.GSBER)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.GSBER) end AS BUS_AREA_LKEY,
RESB.INFNR AS PO_DOC_NBR,
case when length(trim(UPPER((RESB.KDAUF)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.KDAUF)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.KDAUF) end AS SLS_ORD_HDR_KEY,
-- case when length(trim(UPPER((RESB.KDPOS)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.KDPOS)
-- IS NULL then {{var('default_mapkey')}} else UPPER(RESB.KDPOS) end AS SLS_ORD_LN_2_KEY,
case when concat((RESB.KDAUF),'~',(RESB.KDPOS)) = '~' then {{var('default_mapkey')}}
    else concat((RESB.KDAUF),'~',(RESB.KDPOS)) end as SLS_ORD_LN_2_KEY,
case when length(trim(UPPER((RESB.KNTTP)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.KNTTP)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.KNTTP) end AS ACCT_ASGN_CTGY_LKEY,
case when length(trim(UPPER((RESB.KZAUS)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.KZAUS)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.KZAUS) end AS PROD_DISCONTINUATION_TYP_LKEY,
case when length(trim(UPPER((RESB.KZECH)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.KZECH)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.KZECH) end AS BATCH_PROC_MODE_LKEY,
case when length(trim(UPPER((RESB.KZVBR)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.KZVBR)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.KZVBR) end AS CNSMPTN_POST_LKEY,
case when length(trim(UPPER((RESB.LGTYP)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.LGTYP)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.LGTYP) end AS STORG_TYP_LKEY,
RESB.LIFNR as VEND_ID,
case when length(trim(UPPER((RESB.MATKL)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.MATKL)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.MATKL) end AS PROD_GRP_TYP_LKEY,
case when length(trim(UPPER((RESB.NLFMV)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.NLFMV)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.NLFMV) end AS OPER_OFFSET_LEADTIME_UOM_KEY,
case when length(trim(UPPER((RESB.OBJTYPE)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.OBJTYPE)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.OBJTYPE) end AS OBJ_NBR_STAT_LKEY,
RESB.PLNFL AS RTG_SEQ_NBR,
case when length(trim(UPPER((RESB.POSTP)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.POSTP)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.POSTP) end AS BOM_CMPNT_ITEM_CTGY_LKEY,
case when length(trim(UPPER((RESB.PRIO_REQ)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.PRIO_REQ)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.PRIO_REQ) end AS REQ_PRYRT_LKEY,
case when length(trim(UPPER((RESB.PRIO_URG)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.PRIO_URG)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.PRIO_URG) end AS REQ_URGENCY_LKEY,
case when length(trim(UPPER((RESB.PRREG)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.PRREG)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.PRREG) end AS AVAIL_CHK_RULE_NAME_LKEY,
RESB.PSPEL AS WBS_ELMNT_NBR,
case when length(trim(UPPER((RESB.ROKME)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.ROKME)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.ROKME) end AS VARBL_SZ_PROD_UOM_KEY,
case when length(trim(UPPER((RESB.ROMEI)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.ROMEI)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.ROMEI) end AS SZ_UOM_KEY,
case when length(trim(UPPER((RESB.SHKZG)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.SHKZG)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.SHKZG) end AS DEBIT_CREDIT_LKEY,
case when length(trim(UPPER((RESB.STLTY)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.STLTY)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.STLTY) end AS BOM_CTGY_LKEY,
case when length(trim(UPPER((RESB.WERKS)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.WERKS)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.WERKS) end AS PLANT_LOC_KEY,
case when length(trim(UPPER((RESB.RSNUM)))) <1 then {{var('default_mapkey')}} when UPPER(RESB.RSNUM)
IS NULL then {{var('default_mapkey')}} else UPPER(RESB.RSNUM) end AS INVTY_RSRV_HDR_KEY,
RESB.RSNUM as PROD_RSRV_NBR,
RESB.RSPOS as PROD_RSRV_LN_NBR,
RESB.ERFMG as DFLT_QTY,
RESB.ESMNG as USAGE_QTY,
RESB.LGPLA as STORG_SUB_BIN_NBR,
RESB.RSART AS RCRD_TYP, 
RESB.AENNR as RCRD_CHG_NBR,
CASE WHEN RESB.ALPOS = 'X' THEN 'Y' ELSE 'N' END AS ALT_PROD_FLAG, 
RESB.AUFNR as RSRV_ORD_NBR,
RESB.AUFPL as OPER_RTG_NBR,
RESB.AUSCH as CMPNT_SCRAP_PCT,
RESB.AVOAU as OPER_SCRAP_QTY,
RESB.BANFN as PUR_REQ_NBR,
RESB.BDMNG as RESERVED_QTY,
TRY_TO_DATE(RESB.BDTER,'YYYYMMDD') AS PROD_REQ_DTE,  
RESB.BNFPO as PUR_REQ_SEQ_NBR,
RESB.BUDGET_PD as BUD_PERD,
RESB.CHARG as PROD_LOT_BATCH_NBR,
RESB.CHOBJ as ITRL_OBJ_NBR,
RESB.CUOBJ as CONFIGURATION_ITRL_OBJ_NBR,
RESB.DBSKZ AS DIR_PROCUR_TYP, 
CASE WHEN RESB.CLAKZ = 'X' THEN 'Y' ELSE 'N' END AS CLS_FLAG, 
CASE WHEN RESB.DUMPS = 'X' THEN 'Y' ELSE 'N' END AS PHNTM_PROD_FLAG, 
RESB.EBELE as DLV_SCHED_LN_CNTR,
RESB.EBELN as PO_NBR,
RESB.EBELP as PUR_DOC_SEQ_NBR,
RESB.ENMNG as WITHDRAWN_QTY,
RESB.ENWRT as STORG_WITHDRAWN_VAL,
RESB.EWAHR as PROD_USAGE_PRBLTY_PCT,
RESB.FKBER as FUNC_AREA_TEXT,
CASE WHEN RESB.FLGEX = 'X' THEN 'Y' ELSE 'N' END AS EXTNL_PROCUR_FLAG,
RESB.FLMNG as SHORTFALL_QTY,
CASE WHEN RESB.FMENG = 'X' THEN 'Y' ELSE 'N' END AS FIXED_QTY_TYP_FLAG, 
RESB.FPREIS as FOREIGN_CRNCY_FIXED_PRC,
RESB.FPREIS_2 as LCRNCY_FIXED_PRC,
CASE WHEN RESB.FRUNV = 'X' THEN 'Y' ELSE 'N' END AS EXTNL_PROCUR_1_FLAG, 
CASE WHEN RESB.FXPRU = 'X' THEN 'Y' ELSE 'N' END AS IS_FIXED_PRC_CO_PROD_FLAG, 
RESB.GPREIS as CMPNT_CRNCY_PRC,
RESB.GPREIS_2 as LCRNCY_TOT_PRC,
CASE WHEN RESB.HKMAT = 'X' THEN 'Y' ELSE 'N' END AS PROD_REL_ORIG_FLAG, 
RESB.HRKFT as SUBDIV_COST_ELMNT_ORIG_GRP_TEXT,
RESB.INPOS as INTRA_PROD_FLAG,
RESB.KBLNR as EARMARKED_FUNDS_DOC_NBR,
RESB.KBLPOS as EARMARKED_FUND_DOC_SEQ_NBR,
RESB.KBNKZ  AS KANBAN_REPL_TYP, 
CASE WHEN RESB.KFPOS = 'X' THEN 'Y' ELSE 'N' END AS IS_PROD_CONFIGURABLE_FLAG, 
RESB.KNUMH as COND_RCRD_NBR,
CASE WHEN RESB.KTOMA = 'X' THEN 'Y' ELSE 'N' END AS MANUALLY_ENTR_ACCT_FLAG,  
RESB.KZBWS AS VALTN_BASIS_TYP, --M & blank --not a flag
CASE WHEN RESB.KZEAR = 'X' THEN 'Y' ELSE 'N' END AS RSRV_FINAL_ISS_FLAG, 
CASE WHEN RESB.KZKUP = 'X' THEN 'Y' ELSE 'N' END AS CO_PROD_FLAG, --blank
CASE WHEN RESB.KZMPF = 'X' THEN 'Y' ELSE 'N' END AS MANUAL_MTNC_REQ_FLAG, 
RESB.LIFZT as TOT_DLV_DY_CNT,
RESB.LMENG as PROD_REQ_QTY,
CASE WHEN RESB.NAFKZ = 'X' THEN 'Y' ELSE 'N' END AS FOLLOW_UP_PROD_ACTV_FLAG, 
CASE WHEN RESB.NETAU = 'X' THEN 'Y' ELSE 'N' END AS NET_SCRAP_FLAG,  
RESB.NFEAG as DISCONTINUATION_GRP_TEXT,
RESB.NFGRP as FOLLOW_UP_GRP_NBR,
CASE WHEN RESB.NFPKZ = 'X' THEN 'Y' ELSE 'N' END AS FOLLOW_UP_PROD_FLAG, 
RESB.NLFZT as LEADTIME_OFFSET_DY_CNT,
RESB.NLFZV as OPER_OFFSET_LEADTIME_CNT,
CASE WHEN RESB.NO_DISP = 'X' THEN 'Y' ELSE 'N' END AS PROD_PLNG_FLAG, 
RESB.NOMAT AS ORIG_MATL_TYP,
RESB.NOMNG as REQ_QTY, 
RESB.NPTXTKY as ITRL_TEXT,
RESB.OBJNR as OBJ_NBR,
RESB.PBDNR as REQ_PLAN_NBR,
RESB.PEINH as LOT_SZ_IN_FOREIGN_CRNCY,
RESB.PEINH_2 as LOT_SZ_IN_LCRNCY,
RESB.PLNUM as PLND_ORD_NBR,
RESB.PLPLA as STORG_BIN_NBR,
RESB.POSNR as BOM_LN_ITEM_NBR,
RESB.AFPOS as ORD_PROD_DOC_NBR,
RESB.POTX1 as BOM_PROD_1_TEXT, 
RESB.PRVBE as PRODTN_SUPPLY_AREA_TEXT,
RESB.REVLV as PROD_RVN_LVL_NBR,
CASE WHEN RESB.RGEKZ = 'X' THEN 'Y' ELSE 'N' END AS PROD_CMPNT_BACKFLUSH_FLAG, 
RESB.ROANZ as VARBL_SZ_PROD_CNT,
RESB.ROHPS as VARBL_SIZED_PROD_DESC,
RESB.ROMEN as VARBL_SZ_PROD_QTY,
RESB.ROMS1 as SZ_1_DIM_NBR,
RESB.ROMS2 as SZ_2_DIM_NBR,
RESB.ROMS3 as SZ_3_DIM_NBR,
RESB.RSSTA AS RSRV_ENTR_MANUAL_TYP, 
RESB.SAKNR as GL_ACCT_NBR,
CASE WHEN RESB.SANKA = 'X' THEN 'Y' ELSE 'N' END AS COST_RELEVANCY_FLAG, 
TRY_TO_DATE(RESB.SBTER,'YYYYMMDD') AS LATEST_REQ_DTE,
CASE WHEN RESB.SCHGT = 'X' THEN 'Y' ELSE 'N' END AS IS_BULK_MATL_FLAG,
RESB.SERNR as BOM_EXPLD_NBR,
RESB.SGTXT as FREE_TEXT,
RESB.SORTF as SORTING_CMPNT_SRCH_TEXT,
RESB.SPLRV as RSRV_REQ_PROD_NBR,
RESB.STLAL as CHILD_BOM_NAME,
RESB.STLKN as BOM_CMPNT_SEQ_NBR,
RESB.STLNR as BOM_NBR,
RESB.STPOZ as ITRL_CNTR_1_NBR,  
RESB.STVKN as PROD_NODE_NBR,
RESB.TBMNG as RQST_TRNSFR_QTY,
RESB.TECHS as PARM_STD_VARIANT_NBR,
CASE WHEN RESB.TXTPS = 'X' THEN 'Y' ELSE 'N' END AS LN_TEXT_AVAIL_FLAG,
RESB.UMREN as DENOM_BASE_UOM_CONV_NBR,
RESB.UMREZ as NUMERAT_BASE_UOM_CONV_NBR,
RESB.UMSOK AS SPCL_STK_1_TYP, 
CASE WHEN RESB.UPSKZ = 'X' THEN 'Y' ELSE 'N' END AS SUB_PRODS_EXIST_FLAG,
RESB.VMENG as CNFRM_QTY,
CASE WHEN RESB.VORAB = 'X' THEN 'Y' ELSE 'N' END AS PRELIMINARY_ORD_FLAG, 
RESB.VORAB_SM AS ADVANCE_SHIP_TYP, 
RESB.VORNR as ACTVTY_NBR, 
CASE WHEN RESB.VRPLA = 'X' THEN 'Y' ELSE 'N' END AS PLNG_WO_FINAL_ASSEMBLY_FLAG, 
RESB.WEBAZ as GOODS_RCPT_PROC_DY_CNT,
CASE WHEN RESB.XFEHL = 'X' THEN 'Y' ELSE 'N' END AS MSNG_PART_FLAG, 
CASE WHEN RESB.XLOEK = 'X' THEN 'Y' ELSE 'N' END AS DEL_RSRV_ITEM_FLAG, 
CASE WHEN RESB.XWAOK = 'X' THEN 'Y' ELSE 'N' END AS RESERVED_PROD_MVMNT_FLAG,  
RESB.ZUDIV as CUTTING_MEASURES_DIVSR_NBR,
RESB.ZUMS1 as CUTTING_1_MEAS,
RESB.ZUMS2 as CUTTING_2_MEAS,
RESB.ZUMS3 as CUTTING_3_MEAS,
RESB.SOBKZ as SPCL_STK_2_TYP, 
RESB.POTX2 as BOM_PROD_2_TEXT,  
RESB.APLZL as ITRL_CNTR_2_NBR, 
RESB.BDZTP as PROD_REQ_TM,
RESB.FMFGUS_KEY as UNITED_STATES_FED_GOVERNMENT_TEXT
FROM {{source('SAPP10','RESB')}} RESB
WHERE RESB.MANDT={{var('sapp10mandtftr')}}