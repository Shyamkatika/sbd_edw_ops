select
UPPER(CONCAT(coalesce(MSEG.MBLNR::VARCHAR,''),'~',coalesce(MSEG.MJAHR::VARCHAR,''),'~',coalesce(MSEG.ZEILE::VARCHAR,''))) AS INVTY_MVMNT_LN_KEY,
'SAPE03' AS {{var('column_srcsyskey')}},
MD5(UPPER(CONCAT(coalesce(MSEG.MBLNR::VARCHAR,''),'~',coalesce(MSEG.MJAHR::VARCHAR,''),'~',coalesce(MSEG.ZEILE::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
MSEG.LOADDTS AS {{var('column_z3loddtm')}},
MSEG.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},


MSEG.AUFNR AS PRODTN_ORD_NBR,
case when length(trim(UPPER((MSEG.BEMOT)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.BEMOT)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.BEMOT) end AS ACCT_VAL_LKEY,
case when length(trim(UPPER((MSEG.BERKZ)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.BERKZ)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.BERKZ) end AS PRODTN_PLNG_PROD_STG_LKEY,
case when length(trim(UPPER((MSEG.BESTQ)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.BESTQ)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.BESTQ) end AS WHSE_STK_CTGY_LKEY,
case when length(trim(UPPER((MSEG.BPRME)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.BPRME)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.BPRME) end AS PUR_DLV_QTY_UOM_KEY,
MSEG.BSTME AS GOODS_RCPT_QTY_UOM_NBR,
case when length(trim(UPPER((MSEG.BUKRS)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.BUKRS)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.BUKRS) end AS CO_KEY,
case when length(trim(UPPER((MSEG.BWART)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.BWART)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.BWART) end AS STK_MVMNT_TYP_LKEY,
case when length(trim(UPPER((MSEG.BWLVS)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.BWLVS)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.BWLVS) end AS WHSE_MVMNT_TYP_LKEY,
case when length(trim(UPPER((MSEG.BWTAR)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.BWTAR)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.BWTAR) end AS PROD_VALTN_TYP_LKEY,
case when length(trim(UPPER((MSEG.CHARG)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.CHARG)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.CHARG) end AS BATCH_NBR_LKEY,
MSEG.EBELN AS PO_NBR,
MSEG.EMATN AS MFGR_PROD_ID,
MSEG.EMLIF AS VEND_TO_ID,
case when length(trim(UPPER((MSEG.ERFME)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.ERFME)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.ERFME) end AS ENTR_UNIT_QTY_UOM_KEY,
case when length(trim(UPPER((MSEG.EVERE)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.EVERE)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.EVERE) end AS COMPLIANCE_SHIP_INSTRC_LKEY,
case when length(trim(UPPER((MSEG.EVERS)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.EVERS)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.EVERS) end AS SHIP_INSTRC_LKEY,
case when length(trim(UPPER((MSEG.FKBER)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.FKBER)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.FKBER) end AS FUNC_AREA_LKEY,
case when length(trim(UPPER((MSEG.GRUND)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.GRUND)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.GRUND) end AS MVMNT_REASONS_TYP_LKEY,
case when length(trim(UPPER((MSEG.GSBER)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.GSBER)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.GSBER) end AS BUS_AREA_TYP_LKEY,
case when length(trim(UPPER((MSEG.INSMK)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.INSMK)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.INSMK) end AS PROD_STK_TYP_LKEY,
MSEG.KDAUF AS SLS_ORD_NBR,
MSEG.KDEIN AS SLS_ORD_DLV_SCHED_LN_NBR,
MSEG.KDPOS AS SLS_ORD_LN_NBR,
case when length(trim(UPPER((MSEG.KOKRS)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.KOKRS)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.KOKRS) end AS FIN_CTRL_AREA_LKEY,
MSEG.KOSTL AS CSTCTR_ID,
case when length(trim(UPPER((MSEG.KUNNR)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.KUNNR)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.KUNNR) end AS CUST_KEY,
case when length(trim(UPPER((MSEG.KZBEW)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.KZBEW)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.KZBEW) end AS STK_MVMNT_RESN_LKEY,
case when length(trim(UPPER((MSEG.KZBWS)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.KZBWS)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.KZBWS) end AS SPCL_STK_VALTN_LKEY,
case when length(trim(UPPER((MSEG.KZSTR)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.KZSTR)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.KZSTR) end AS MVMNT_STATS_GENERATED_LKEY,
case when length(trim(UPPER((MSEG.KZVBR)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.KZVBR)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.KZVBR) end AS CNSMPTN_POST_LKEY,
case when length(trim(UPPER((MSEG.KZZUG)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.KZZUG)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.KZZUG) end AS ACCT_TREATMENT_LKEY,
case when length(trim(UPPER((MSEG.LGNUM)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.LGNUM)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.LGNUM) end AS WHSE_LKEY,
MSEG.LGORT AS SLOC_ID,
case when length(trim(UPPER((MSEG.LGTYP)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.LGTYP)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.LGTYP) end AS STORG_TYP_LKEY,
case when length(trim(UPPER((MSEG.LSMEH)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.LSMEH)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.LSMEH) end AS DLV_UOM_KEY,
MSEG.MATBF STK_MANAGED_PROD_NBR,
case when length(trim(UPPER((MSEG.MATNR)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.MATNR)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.MATNR) end AS PROD_KEY,
MSEG.MAT_PSPNR AS SLS_ORD_STK_WBS_NBR,
case when length(trim(UPPER((MSEG.MEINS)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.MEINS)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.MEINS) end AS PROD_BASE_UOM_KEY,
case when length(trim(UPPER((MSEG.MWSKZ)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.MWSKZ)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.MWSKZ) end AS TAX_CD_LKEY,
case when length(trim(UPPER((MSEG.PARBU)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.PARBU)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.PARBU) end AS CLEARING_CO_LKEY,
case when length(trim(UPPER((MSEG.PARGB)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.PARGB)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.PARGB) end AS TRDG_PTNR_BUS_AREA_LKEY,
MSEG.PPRCTR AS PTNR_FIN_PROFIT_CNTR_ID,
MSEG.PRCTR AS FIN_PROFIT_CNTR_ID,
MSEG.PS_PSP_PNR AS WORK_BRKDN_STRC_NBR,
case when length(trim(UPPER((MSEG.QINSPST)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.QINSPST)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.QINSPST) end AS GOOD_RCPT_INSPCT_STAT_LKEY,
case when length(trim(UPPER((MSEG.SOBKZ)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.SOBKZ)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.SOBKZ) end AS SPCL_STK_ITEM_LKEY,
case when length(trim(UPPER((MSEG.SPE_GTS_STOCK_TY)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.SPE_GTS_STOCK_TY)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.SPE_GTS_STOCK_TY) end AS GTS_STK_TYP_LKEY,
MSEG.TANUM AS TRNSFR_ORD_NBR,
case when length(trim(UPPER((MSEG.UMBAR)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.UMBAR)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.UMBAR) end AS PROD_SPLIT_VALTN_TYP_LKEY,
MSEG.UMLGO AS RCV_ISS_SLOC_ID,
MSEG.UMMAB AS RCV_ISS_PROD_ID,
case when length(trim(UPPER((MSEG.UMSOK)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.UMSOK)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.UMSOK) end AS SPCL_STK_TYP_LKEY,
case when length(trim(UPPER((MSEG.UMWRK)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.UMWRK)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.UMWRK) end AS RCV_ISS_PLANT_LOC_KEY,
case when length(trim(UPPER((MSEG.VGART_MKPF)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.VGART_MKPF)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.VGART_MKPF) end AS MVMNT_TYP_LKEY,
case when length(trim(UPPER((MSEG.VPRSV)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.VPRSV)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.VPRSV) end AS PRC_CTRL_TYP_LKEY,
case when length(trim(UPPER((MSEG.WAERS)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.WAERS)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.WAERS) end AS CRNCY_KEY,
case when length(trim(UPPER((MSEG.WERKS)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.WERKS)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.WERKS) end AS PLANT_LOC_KEY,
case when length(trim(UPPER((MSEG.ZUSTD_T156M)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.ZUSTD_T156M)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.ZUSTD_T156M) end AS STK_TYP_MOD_LKEY,
case when length(trim(UPPER((MSEG._BEV2_ED_KZ_VER)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG._BEV2_ED_KZ_VER)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG._BEV2_ED_KZ_VER) end AS PROC_STAT_LKEY,
MSEG.FISTL AS BUD_ASGN_FUNDS_CNTR_ID,
MSEG.GEBER AS BUD_FUND_ID,
MSEG.GRANT_NBR AS GRANT_ID,
case when length(trim(UPPER((MSEG.J_1AGIRUPD)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.J_1AGIRUPD)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.J_1AGIRUPD) end AS GOODS_ISS_REVALUATION_RELVNT_LKEY,
case when length(trim(UPPER((MSEG.LSTAR)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.LSTAR)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.LSTAR) end AS FIN_CTRL_AREA_ACTVTY_LKEY,
MSEG.PLPLA AS STORG_BIN_ID,
case when length(trim(UPPER((MSEG.VPTNR)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.VPTNR)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.VPTNR) end AS JOINT_VENTURE_PTNR_ACCT_KEY,
case when length(trim(UPPER((MSEG.XMACC)))) <1 then {{var('default_mapkey')}} when UPPER(MSEG.XMACC)
IS NULL then {{var('default_mapkey')}} else UPPER(MSEG.XMACC) end AS MULT_ACCT_ASGN_LKEY,


case when UPPER(MSEG.DYPLA) ='X' then'Y' else'N' end AS DYN_STORG_BIN_FLAG,
case when UPPER(MSEG.ELIKZ) ='X' then'Y' else'N' end AS DLV_IS_CMPLT_FLAG,
case when UPPER(MSEG.KZEAR) ='X' then'Y'else'N' end AS FINAL_GOODS_ISS_FLAG,
case when UPPER(MSEG.MENGU) ='X' then'Y'else'N' end AS PROD_MANAGED_BY_QTY_FLAG,
case when UPPER(MSEG.NSCHN) ='X' then'Y'else'N' end AS INTERIM_STORG_POST_FLAG,
case when UPPER(MSEG.VSCHN) ='X' then'Y'else'N' end AS INTERIM_STORG_BIN_POST_FLAG,
case when UPPER(MSEG.WERTU) ='X' then'Y'else'N' end AS PROD_VAL_UPD_FLAG,
case when UPPER(MSEG.WEUNB) ='X' then'Y'else'N' end AS GOODS_RCPT_VALUATED_FLAG,
case when UPPER(MSEG.XBLVS) ='X' then'Y'else'N' end AS POST_IN_WHSE_MGMT_SYS_FLAG,
case when UPPER(MSEG.XRUEJ) ='X' then'Y'else'N' end AS BK_POST_YR_FLAG,
case when UPPER(MSEG.XRUEM) ='X' then'Y'else'N' end AS BK_POST_PERD_FLAG,
case when UPPER(MSEG.XWSBR) ='X' then'Y'else'N' end AS GOOD_RCPT_REVERSAL_FLAG,
case when UPPER(MSEG.ZUSTD) ='X' then'Y'else'N' end AS RESTRCT_USE_BATCH_FLAG,
case when UPPER(MSEG.XBEAU) ='X' then'Y'else'N' end AS PO_CREATE_ON_GOODS_RCPT_FLAG,
case when UPPER(MSEG.XOBEW) ='X' then'Y'else'N' end AS VEND_STK_VALTN_FLAG,
case when UPPER(MSEG.XSAUF) ='X' then'Y'else'N' end AS MVMNT_IS_STATS_FLAG,
case when UPPER(MSEG.XSERG) ='X' then'Y'else'N' end AS PROFITABILITY_ANLYS_IS_STATS_FLAG,
case when UPPER(MSEG.XSKST) ='X' then'Y'else'N' end AS CSTCTR_RELVNT_IS_STATS_FLAG,
case when UPPER(MSEG.XSPRO) ='X' then'Y'else'N' end AS PROJ_RELVNT_IS_STATS_FLAG,
case when UPPER(MSEG.XWOFF) ='X' then'Y'else'N' end AS CALC_OF_VAL_OPN_FLAG,


TRY_TO_TIMESTAMP_NTZ(CONCAT(TRY_TO_DATE(MSEG.CPUDT_MKPF,'YYYYMMDD'),' ',
            TRY_TO_TIME((case when length(trim(MSEG.CPUTM_MKPF)) <1 then '000000'
when MSEG.CPUTM_MKPF IS NULL then '000000'
else MSEG.CPUTM_MKPF end),'HH24MISS'))) as ENTR_TM,

TRY_TO_TIMESTAMP_NTZ(CONCAT(TRY_TO_DATE(MSEG._BEV2_ED_AEDAT,'YYYYMMDD'),' ',
            TRY_TO_TIME((case when length(trim(MSEG._BEV2_ED_AETIM)) <1 then '000000'
when MSEG._BEV2_ED_AETIM IS NULL then '000000'
else MSEG._BEV2_ED_AETIM end),'HH24MISS'))) as EXCISE_DUTY_CURR_SYS_TM,



TRY_TO_DATE(MSEG._BEV2_ED_AEDAT,'YYYYMMDD') AS EXCISE_DUTY_LAST_UPD_DTE,
TRY_TO_DATE(MSEG.BUDAT_MKPF,'YYYYMMDD') AS FIN_ACCT_DTE,
TRY_TO_DATE(MSEG.DABRBZ,'YYYYMMDD') AS SETLMNT_REF_1_DTE,
TRY_TO_DATE(MSEG.DABRZ,'YYYYMMDD') AS SETLMNT_REF_2_DTE,
TRY_TO_DATE(MSEG.HSDAT,'YYYYMMDD') AS PROD_MFG_DTE,
TRY_TO_DATE(MSEG.VFDAT,'YYYYMMDD') AS SHELF_LIFE_EXPR_DTE,
TRY_TO_DATE(MSEG.CPUDT_MKPF,'YYYYMMDD') AS ACCT_DOC_DTE_ENTR_TM,

MSEG.ABLAD AS PROD_UNLOADED_POINT_NAME,
MSEG.ANLN1 AS MAIN_ASSET_NBR,
MSEG.ANLN2 AS SUB_MAIN_ASSET_NBR,
MSEG.APLZL AS SEQ_NBR,
MSEG.AUFPL AS PRODTN_ORD_RTG_NBR,
MSEG.AUFPS AS PROD_ORD_LN_NBR,
MSEG.BNBTR AS DLV_LCRNCY_COST,
MSEG.BPMNG AS PUR_DLV_QTY,
MSEG.BSTMG AS GOODS_RCPT_QTY,
MSEG.BUALT AS ALT_PRC_CTRL_AMT,
MSEG.BUSTM AS QTY_TEXT,
MSEG.BUSTW AS VAL_TEXT,
MSEG.BUZEI AS ACCT_DOC_LN_1_NBR,
MSEG.BUZUM AS ACCT_DOC_LN_2_NBR,
MSEG.CUOBJ_CH AS SEQ_BATCH_NBR,
MSEG.DMBTR AS LCRNCY_AMT,
MSEG.DMBUM AS REVALUATION_AMT,
MSEG.EBELP AS PO_LN_NBR,
MSEG.ERFMG AS ENTR_UNIT_QTY,
MSEG.EXBWR AS EXTNL_POST_LCRNCY_AMT,
MSEG.EXVKW AS EXTNL_SLS_LCRNCY_AMT,
MSEG.GJAHR AS PROD_MVMNT_DOC_LN_NBR_YR,
MSEG.J_1BEXBASE AS NOTA_FSCL_ALT_BASE_AMT,
MSEG.KBLPOS AS EARMARKED_FUNDS_LN_NBR,
MSEG.LBKUM AS VALUATED_STK_QTY,
MSEG.LFBJA AS REF_DOC_FYR,
MSEG.LFBNR AS REF_DOC_NBR,
MSEG.LFPOS AS REF_DOC_ITEM_NBR,
MSEG.LGPLA AS WHSE_STORG_SUB_LOC_NAME,
MSEG.LINE_DEPTH AS PROD_MVMNT_DOC_HIER_NBR,
MSEG.LINE_ID AS PROD_MVMNT_DOC_LN_SEQ_NBR,
MSEG.LSMNG AS UOM_QTY,
MSEG.MAA_URZEI AS ACCT_ASGN_ORIG_LN_NBR,
MSEG.MBLNR AS PROD_MVMNT_DOC_NBR,
MSEG.MENGE AS GOODS_RCPT_ISS_PROD_QTY,
MSEG.MJAHR AS PROD_MVMNT_DOC_YR,
MSEG.NPLNR AS PROJ_NETWORK_NBR,
MSEG.OINAVNW AS NON_DEDUCTIBLE_TAX_AMT,
MSEG.PALAN AS WMS_PAL_QTY,
MSEG.PAOBJNR AS PROFITABILITY_ANLYS_SEG_NBR,
MSEG.PARENT_ID AS PRNT_PROD_MVMNT_DOC_LN_NBR,
MSEG.PBAMG AS ACCT_ASGN_PROD_QTY,
MSEG.PROJN AS PROJ_NBR,
MSEG.RSART AS RCRD_TYP_TEXT,
MSEG.RSNUM AS PROD_RSRV_NBR,
MSEG.RSPOS AS PROD_RSRV_LN_NBR,
MSEG.SAKTO AS GL_ACCT_NBR,
MSEG.SALK3 AS PROD_VALUATED_STK_PRC,
MSEG.SGTXT AS FREE_TEXT,
MSEG.SHKUM AS REVALUATION_TYP,
MSEG.SHKZG AS REVALUATION_DEBIT_CREDIT_TYP,
MSEG.SJAHR AS REVALUATION_PROD_DOC_YR,
MSEG.SMBLN AS REVALUATION_PROD_DOC_NBR,
MSEG.SMBLP AS REVALUATION_PROD_DOC_LN_NBR,
MSEG.TBNUM AS TRNSFR_REQ_NBR,
MSEG.TBPOS AS TRNSFR_REQ_SEQ_NBR,
MSEG.TCODE2_MKPF AS INVTY_MVMNT_LN_CREATE_PROC_TEXT,
MSEG.TXJCD AS TAX_JURSDN_RATE,
MSEG.UBNUM AS POST_CHG_NBR,
MSEG.UMCHA AS RCV_ISS_BATCH_NBR,
MSEG.URZEI AS ACCT_ASGN_ORIG_LN_DOC_TEXT,
MSEG.USNAM_MKPF AS TXN_USER_NAME,
MSEG.VBELN_IM AS SLS_DLV_NBR,
MSEG.VBELP_IM AS SLS_DLV_ITEM_NBR,
MSEG.VKWRA AS SLS_EXCLUDED_TAX_PRC,
MSEG.VKWRT AS SLS_INCLD_TAX_PRC,
MSEG.WEANZ AS GOODS_RCPT_SLIPS_PRINTED_CNT,
MSEG.WEMPF AS GOODS_RCPNT_NAME,
MSEG.XAUTO AS AUTO_PROD_CREATE_TYP,
MSEG.ZEILE AS PROD_MVMNT_DOC_LN_NBR,
MSEG.ZEKKN AS ACCT_ASGN_SEQ_NBR,
MSEG.ZUSCH AS BATCH_STAT_TEXT,
MSEG.BELNR AS DOC_ACCT_1_NBR,
MSEG.BELUM AS DOC_ACCT_2_NBR,
MSEG.KBLNR AS EARMARKED_FUND_DOC_NBR,
MSEG._BEV2_ED_USER AS PROC_USER_NAME_TEXT,
MSEG.AKTNR AS SLS_PROMO_NBR,
MSEG.CONDI AS JOINT_VENTURE_CONDITION2_NBR,
MSEG.EQUNR AS EQUIP_NBR,
MSEG.FIPOS AS FIN_MGMT_COMMIT_NBR,
MSEG.IMKEY AS REAL_ESTATE_OBJ_NBR,
MSEG.KSTRG AS COST_OBJ_NBR,
MSEG.OICONDCOD AS JOINT_VENTURE_CONDITION1_NBR,
MSEG.PRZNR AS BUS_PROC_NBR,
MSEG.TBPRI AS TRNSFR_REQ_PRYRT_NBR,
MSEG.UMZST AS RESTRCT_USE_BATCH_TRNSFR_TYP,
MSEG.UMZUS AS BATCH_TRNSFR_TYP,
MSEG.VKMWS AS TAX_NBR,
MSEG.LIFNR AS VEND_ACCT_NBR,
MSEG.MAT_KDAUF AS VALUATED_STK_SLS_ORD_NBR,
MSEG.MAT_KDPOS AS VALUATED_SLS_ITEM_NAME,

case when concat((MSEG.MBLNR),'~',(MSEG.MJAHR)) = '~' then {{var('default_mapkey')}}
    else concat((MSEG.MBLNR),'~',(MSEG.MJAHR)) end as INVTY_MVMNT_HDR_KEY

FROM {{source('SAPE03','MSEG')}} MSEG
WHERE MSEG.MANDT={{var('sape03mandtftr')}}