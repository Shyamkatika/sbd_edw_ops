{{ config(materialized='table' , transient=false, full_refresh=true, schema='EDW_DBT_VIEWS' )}}

WITH DIM_PRODUCT_LOC_VALUATION_SAPC11 AS 
(SELECT PROD_KEY,VALTN_AREA_LKEY,VALTN_TYP_LKEY,FYR_NBR,FMTH_NBR,STANDARD_PRICE,UNIT_PRICE,VALTN_CLASS,CO_KEY,CRNCY_KEY,SRC_SYS_KEY 
FROM (SELECT PROD_KEY,VALTN_AREA_LKEY,VALTN_TYP_LKEY,FYR_NBR,FMTH_NBR,STANDARD_PRICE,UNIT_PRICE,VALTN_CLASS,CO_KEY,CRNCY_KEY,SRC_SYS_KEY FROM(
select PROD_KEY,VALTN_AREA_LKEY,VALTN_TYP_LKEY,FYR_NBR,FMTH_NBR,STANDARD_PRICE,UNIT_PRICE,VALTN_CLASS,CO_KEY,CRNCY_KEY,SRC_SYS_KEY,
ROW_NUMBER() OVER (PARTITION BY PROD_KEY,VALTN_AREA_LKEY,FYR_NBR,FMTH_NBR,SRC_SYS_KEY ORDER BY EFF_DTE DESC) AS row_number FROM {{source('DIMENSIONS','DIM_PRODUCT_LOC_VALUATION')}})
WHERE row_number=1) WHERE SRC_SYS_KEY='SAPC11')

SELECT
OH.INVTY_ON_HAND_SNAPSHOT_KEY AS INVTY_ON_HAND_SNAPSHOT_KEY
,OH.SRC_SYS_KEY
,OH.SRC_RCRD_CREATE_DTE
,OH.SRC_RCRD_CREATE_USERID
,OH.SRC_RCRD_UPD_DTE
,OH.SRC_RCRD_UPD_USERID
,OH.RCRD_HASH_KEY
,OH.DEL_FROM_SRC_FLAG
,'{{model.name}}' AS {{var('column_ETL_INS_PID')}}
,CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}}
,'{{model.name}}' AS {{var('column_ETL_UPD_PID')}}
,CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}}
,OH.EFF_DTE_RN::TIMESTAMP_NTZ  AS EFF_DTE
,OH.EXPR_DTE_RN::TIMESTAMP_NTZ AS EXPR_DTE
,OH.ETL_UPD_DTE AS ZONE4_ETL_UPD_DTE
,OH.ETL_INS_PID AS ZONE4_ETL_INS_PID
,OH.PROD_DFLT_LOC_KEY
,OH.ZONE3_LOD_DTE
,OH.PROD_KEY
,OH.LOC_KEY
,OH.RESTRCT_PROD_BY_LOC_TYP_LKEY
,OH.INVTY_GRADE_LKEY
,OH.INVTY_CRCTD_FACTOR_LKEY
,OH.SLOC_MRP_LKEY
,OH.INVTY_RESTRCT_USE_STK_STAT_LKEY
,OH.INVTY_WHSE_USE_STK_STAT_LKEY
,OH.INVTY_QLTY_INSPCT_STK_STAT_LKEY
,OH.INVTY_BLKD_STK_STAT_LKEY
,OH.PAST_INVTY_RESTRCT_USE_STK_STAT_LKEY
,OH.PAST_INVTY_WHSE_USE_STK_STAT_LKEY
,OH.PAST_INVTY_QLTY_INSPCT_STK_STAT_LKEY
,OH.PAST_INVTY_BLKD_STK_STAT_LKEY
,OH.SPCL_PROCUR_TYP_LKEY
,OH.MTNC_STAT_TYP_LKEY
,OH.CURR_PROD_COST_LKEY
,OH.MRP_PROC_FLAG
,OH.ROLL_FLAG
,OH.SLOC_LVL_DEL_FLAG
,OH.PAST_RCRD_EXISTS_FLAG
,OH.PROD_RCV_DTE
,OH.PROD_ADD_LOC_DTE
,OH.LAST_STOCKROOM_ISS_DTE
,OH.LAST_STOCKROOM_RCPT_DTE
,OH.CYCLE_CNT_DTE
,OH.SLS_AND_GOODS_ISS_CALC_DTE
,OH.UNRSTR_USE_STK_LAST_CNT_DTE
,OH.REF_WORK_ORD_NBR
,OH.INVTY_LVL_NBR
,OH.CURR_CLNDR_PERD_NBR
--,OH.PAST_RESTRCT_BATCHES_TOT_QTY
--,OH.PAST_INSPCT_STK_QTY AS INSPCT_STK_QTY
--,(OH.PAST_INSPCT_STK_QTY * (CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) 
--ELSE 0 END)) AS INSPCT_STK_AMT
--,OH.PAST_VALUATED_STK_QTY AS VALUATED_STK_QTY
--,OH.PAST_BLKD_STK_RTRN_QTY
--,OH.PAST_BLKD_STK_QTY
--,OH.PAST_STK_IN_TRNSFR_QTY  AS STK_IN_TRNSFR_QTY
--,(OH.PAST_STK_IN_TRNSFR_QTY * (CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) 
--ELSE 0 END)) AS STK_IN_TRNSFR_AMT
,OH.PROD_MAX_ORD_QTY
,(OH.PROD_MAX_ORD_QTY * (CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) 
ELSE 0 END)) AS PROD_MAX_ORD_AMT
,OH.PROD_ALLOC_QTY
,(OH.PROD_ALLOC_QTY * (CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) 
ELSE 0 END)) AS PROD_ALLOC_AMT
,OH.PROD_BKORD_QTY
,(OH.PROD_BKORD_QTY * (CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) 
ELSE 0 END)) AS PROD_BKORD_AMT
,OH.PROD_ON_HAND_QTY
,(OH.PROD_ON_HAND_QTY * (CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) 
ELSE 0 END)) AS PROD_ON_HAND_AMT
,OH.PROD_MIN_ORD_QTY
,(OH.PROD_MIN_ORD_QTY * (CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) 
ELSE 0 END)) AS PROD_MIN_ORD_AMT
,OH.PROD_NET_ON_HAND_QTY
,(OH.PROD_NET_ON_HAND_QTY * (CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) 
ELSE 0 END)) AS PROD_NET_ON_HAND_AMT
,OH.PROD_REQ_QTY
,(OH.PROD_REQ_QTY * (CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) 
ELSE 0 END)) AS PROD_REQ_AMT
,OH.PROD_ORD_QTY
,(OH.PROD_ORD_QTY * (CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) 
ELSE 0 END)) AS PROD_ORD_AMT
,OH.PROD_REORD_QTY
,(OH.PROD_REORD_QTY * (CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) 
ELSE 0 END)) AS PROD_REORD_AMT
,OH.PROD_NON_NETTABLE_ON_HAND_QTY
,(OH.PROD_NON_NETTABLE_ON_HAND_QTY * (CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) 
ELSE 0 END)) AS PROD_NON_NETTABLE_ON_HAND_AMT
,OH.ANTICIPATED_PROD_QTY
,(OH.ANTICIPATED_PROD_QTY * (CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) 
ELSE 0 END)) AS ANTICIPATED_PROD_AMT
,CASE WHEN OH.ETL_INS_PID LIKE '%HIST%' THEN OH.PAST_RESTRCT_BATCHES_TOT_QTY ELSE OH.RESTRCT_BATCHES_TOT_QTY END AS RESTRCT_BATCHES_TOT_QTY
,((CASE WHEN OH.ETL_INS_PID LIKE '%HIST%' THEN OH.PAST_RESTRCT_BATCHES_TOT_QTY ELSE OH.RESTRCT_BATCHES_TOT_QTY END) * 
(CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) 
ELSE 0 END)) AS RESTRCT_BATCHES_TOT_AMT
,CASE WHEN OH.ETL_INS_PID LIKE '%HIST%' THEN OH.PAST_INSPCT_STK_QTY ELSE OH.QLTY_INSPCT_STK_QTY END AS QLTY_INSPCT_STK_QTY
,((CASE WHEN OH.ETL_INS_PID LIKE '%HIST%' THEN OH.PAST_INSPCT_STK_QTY ELSE OH.QLTY_INSPCT_STK_QTY END) * 
(CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) ELSE 0 END)) AS QLTY_INSPCT_STK_AMT
,OH.RESTRCT_USE_STK_QTY
,(OH.RESTRCT_USE_STK_QTY * 
(CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) 
ELSE 0 END)) AS RESTRCT_USE_STK_AMT
,OH.QLTY_INSPCT_CNSGN_STK_QTY
,(OH.QLTY_INSPCT_CNSGN_STK_QTY * (CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) 
ELSE 0 END)) AS QLTY_INSPCT_CNSGN_STK_AMT
,OH.UNRSTR_USE_STK_QTY
,(OH.UNRSTR_USE_STK_QTY * (CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) 
ELSE 0 END)) AS UNRSTR_USE_STK_AMT
,OH.BLKD_CNSGN_STK_QTY
,(OH.BLKD_CNSGN_STK_QTY * (CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) 
ELSE 0 END)) AS BLKD_CNSGN_STK_AMT
,CASE WHEN OH.ETL_INS_PID LIKE '%HIST%' THEN OH.PAST_VALUATED_STK_QTY ELSE OH.VALUATED_UNRSTR_STK_QTY END AS VALUATED_UNRSTR_STK_QTY
,((CASE WHEN OH.ETL_INS_PID LIKE '%HIST%' THEN OH.PAST_VALUATED_STK_QTY ELSE OH.VALUATED_UNRSTR_STK_QTY END) * 
(CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) ELSE 0 END)) AS VALUATED_UNRSTR_STK_AMT
,OH.REPL_QTY
,(OH.REPL_QTY * (CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) ELSE 0 END)) AS REPL_AMT
,OH.REORD_POINT_LVL_QTY
,(OH.REORD_POINT_LVL_QTY * (CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) 
ELSE 0 END)) AS REORD_POINT_LVL_AMT
,CASE WHEN OH.ETL_INS_PID LIKE '%HIST%' THEN OH.PAST_BLKD_STK_RTRN_QTY ELSE OH.BLKD_STK_RTRN_QTY END AS BLKD_STK_RTRN_QTY
,((CASE WHEN OH.ETL_INS_PID LIKE '%HIST%' THEN OH.PAST_BLKD_STK_RTRN_QTY ELSE OH.BLKD_STK_RTRN_QTY END) * 
(CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) 
ELSE 0 END)) AS BLKD_STK_RTRN_AMT
,CASE WHEN OH.ETL_INS_PID LIKE '%HIST%' THEN OH.PAST_BLKD_STK_QTY ELSE OH.BLKD_STK_QTY END AS BLKD_STK_QTY
,(CASE WHEN OH.ETL_INS_PID LIKE '%HIST%' THEN OH.PAST_BLKD_STK_QTY ELSE OH.BLKD_STK_QTY END * 
(CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) ELSE 0 END)) AS BLKD_STK_AMT
,CASE WHEN OH.ETL_INS_PID LIKE '%HIST%' THEN OH.PAST_STK_IN_TRNSFR_QTY ELSE OH.TRANSFERRED_STK_QTY END AS TRANSFERRED_STK_QTY
,((CASE WHEN OH.ETL_INS_PID LIKE '%HIST%' THEN OH.PAST_STK_IN_TRNSFR_QTY ELSE OH.TRANSFERRED_STK_QTY END) * 
(CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) ELSE 0 END)) AS TRANSFERRED_STK_AMT
,OH.PAST_PERD_RESTRCT_STK_QTY AS PAST_PERD_RESTRCT_STK_QTY
,(OH.PAST_PERD_RESTRCT_STK_QTY * (CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) 
ELSE 0 END)) AS PAST_PERD_RESTRCT_STK_AMT
,OH.PAST_PERD_QLTY_INSPCT_QTY AS PAST_PERD_QLTY_INSPCT_QTY
,(OH.PAST_PERD_QLTY_INSPCT_QTY * (CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) 
ELSE 0 END)) AS PAST_PERD_QLTY_INSPCT_AMT
,OH.PAST_PERD_UNRSTR_USE_STK_QTY AS PAST_PERD_UNRSTR_USE_STK_QTY
,(OH.PAST_PERD_UNRSTR_USE_STK_QTY * (CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) 
ELSE 0 END)) AS PAST_PERD_UNRSTR_USE_STK_AMT
,OH.PAST_PERD_BLKD_STK_RTRN_QTY AS PAST_PERD_BLKD_STK_RTRN_QTY
,(OH.PAST_PERD_BLKD_STK_RTRN_QTY * (CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) 
ELSE 0 END)) AS PAST_PERD_BLKD_STK_RTRN_AMT
,OH.PAST_PERD_BLKD_STK_QTY AS PAST_PERD_BLKD_STK_QTY
,(OH.PAST_PERD_BLKD_STK_QTY * (CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) 
ELSE 0 END)) AS PAST_PERD_BLKD_STK_AMT,
OH.PAST_PERD_TRNSFR_STK_QTY AS PAST_PERD_TRNSFR_STK_QTY
,(OH.PAST_PERD_TRNSFR_STK_QTY * (CASE WHEN OH.STANDARD_PRICE<>0 AND OH.UNIT_PRICE <>0 THEN (OH.STANDARD_PRICE/OH.UNIT_PRICE) 
ELSE 0 END)) AS PAST_PERD_TRNSFR_STK_AMT
--,OH.PAST_PROD_STK_VAL
--,OH.PAST_SLS_VAL
,OH.DIV_ID
,OH.PROD_RCV_YR_ID
,OH.INVTY_STAT_TYP
,OH.MISC_TYP
,OH.TOT_SLS_RCPT_AMT
,OH.TOT_SLS_ISS_AMT
,OH.AVG_GOODS_ISS_PER_DY_CNT
,OH.AVG_SLS_PER_DY_AMT
,OH.PROD_ASSAY_PCT
,OH.ORIG_CNTRY_ID
,OH.CURR_FYR
,OH.SLOC_ID
,OH.STORG_BIN_ID
,OH.PICK_AREA_FOR_LEAN_WM_TYP
,OH.PHY_INVTY_FYR
,OH.FIN_PROFIT_CNTR_ID
,OH.PHY_INVTY_BLOCK_TYP
,CASE WHEN OH.ETL_INS_PID LIKE '%HIST%' THEN OH.PAST_PROD_STK_VAL ELSE OH.VALUATED_STK_SLS_VAL_AMT END AS VALUATED_STK_SLS_VAL_AMT
,CASE WHEN OH.ETL_INS_PID LIKE '%HIST%' THEN OH.PAST_SLS_VAL ELSE OH.STK_TRNSFR_SLS_VAL_AMT END AS STK_TRNSFR_SLS_VAL_AMT
,OH.GL_PROD_COST_TYP
,OH.SUB_LOC_TYP
,OH.PAST_FYR_TM
,OH.PAST_FSCL_PERD_TM
,OH.PROD_ADD_LOC_YR_ID
,OH.PROD_REL_SET_ITEM
--,OH.PROD_KEY
--,OH.VALTN_AREA_LKEY
--,OH.VALTN_TYP_LKEY
--,OH.FYR_NBR
--,OH.FMTH_NBR
,OH.STANDARD_PRICE
,OH.UNIT_PRICE
,OH.VALTN_CLASS
,OH.CO_KEY
,OH.CRNCY_KEY
FROM 
(SELECT * FROM 
(SELECT OH.INVTY_ON_HAND_SNAPSHOT_KEY
,OH.SRC_SYS_KEY
,OH.SRC_RCRD_CREATE_DTE
,OH.SRC_RCRD_CREATE_USERID
,OH.SRC_RCRD_UPD_DTE
,OH.SRC_RCRD_UPD_USERID
,OH.RCRD_HASH_KEY
,OH.DEL_FROM_SRC_FLAG
,OH.EFF_DTE_RN 
,OH.EXPR_DTE_RN 
,OH.ETL_UPD_DTE 
,OH.ETL_INS_PID 
,OH.PROD_DFLT_LOC_KEY
,OH.ZONE3_LOD_DTE
,OH.PROD_KEY
,OH.LOC_KEY
,OH.RESTRCT_PROD_BY_LOC_TYP_LKEY
,OH.INVTY_GRADE_LKEY
,OH.INVTY_CRCTD_FACTOR_LKEY
,OH.SLOC_MRP_LKEY
,OH.INVTY_RESTRCT_USE_STK_STAT_LKEY
,OH.INVTY_WHSE_USE_STK_STAT_LKEY
,OH.INVTY_QLTY_INSPCT_STK_STAT_LKEY
,OH.INVTY_BLKD_STK_STAT_LKEY
,OH.PAST_INVTY_RESTRCT_USE_STK_STAT_LKEY
,OH.PAST_INVTY_WHSE_USE_STK_STAT_LKEY
,OH.PAST_INVTY_QLTY_INSPCT_STK_STAT_LKEY
,OH.PAST_INVTY_BLKD_STK_STAT_LKEY
,OH.SPCL_PROCUR_TYP_LKEY
,OH.MTNC_STAT_TYP_LKEY
,OH.CURR_PROD_COST_LKEY
,OH.MRP_PROC_FLAG
,OH.ROLL_FLAG
,OH.SLOC_LVL_DEL_FLAG
,OH.PAST_RCRD_EXISTS_FLAG
,OH.PROD_RCV_DTE
,OH.PROD_ADD_LOC_DTE
,OH.LAST_STOCKROOM_ISS_DTE
,OH.LAST_STOCKROOM_RCPT_DTE
,OH.CYCLE_CNT_DTE
,OH.SLS_AND_GOODS_ISS_CALC_DTE
,OH.UNRSTR_USE_STK_LAST_CNT_DTE
,OH.REF_WORK_ORD_NBR
,OH.INVTY_LVL_NBR
,OH.CURR_CLNDR_PERD_NBR
,OH.PAST_RESTRCT_BATCHES_TOT_QTY
,OH.PAST_INSPCT_STK_QTY
,OH.PAST_VALUATED_STK_QTY
,OH.PAST_BLKD_STK_RTRN_QTY
,OH.PAST_BLKD_STK_QTY
,OH.PAST_STK_IN_TRNSFR_QTY
,OH.PROD_MAX_ORD_QTY
,OH.PROD_ALLOC_QTY
,OH.PROD_BKORD_QTY
,OH.PROD_ON_HAND_QTY
,OH.PROD_MIN_ORD_QTY
,OH.PROD_NET_ON_HAND_QTY
,OH.PROD_REQ_QTY
,OH.PROD_ORD_QTY
,OH.PROD_REORD_QTY
,OH.PROD_NON_NETTABLE_ON_HAND_QTY
,OH.ANTICIPATED_PROD_QTY
,OH.RESTRCT_BATCHES_TOT_QTY
,OH.QLTY_INSPCT_STK_QTY
,OH.RESTRCT_USE_STK_QTY
,OH.QLTY_INSPCT_CNSGN_STK_QTY
,OH.UNRSTR_USE_STK_QTY
,OH.BLKD_CNSGN_STK_QTY
,OH.VALUATED_UNRSTR_STK_QTY
,OH.REPL_QTY
,OH.REORD_POINT_LVL_QTY
,OH.BLKD_STK_RTRN_QTY
,OH.BLKD_STK_QTY
,OH.TRANSFERRED_STK_QTY
,OH.PAST_PERD_RESTRCT_STK_QTY
,OH.PAST_PERD_QLTY_INSPCT_QTY
,OH.PAST_PERD_UNRSTR_USE_STK_QTY
,OH.PAST_PERD_BLKD_STK_RTRN_QTY
,OH.PAST_PERD_BLKD_STK_QTY
,OH.PAST_PERD_TRNSFR_STK_QTY
,OH.PAST_PROD_STK_VAL
,OH.PAST_SLS_VAL
,OH.DIV_ID
,OH.PROD_RCV_YR_ID
,OH.INVTY_STAT_TYP
,OH.MISC_TYP
,OH.TOT_SLS_RCPT_AMT
,OH.TOT_SLS_ISS_AMT
,OH.AVG_GOODS_ISS_PER_DY_CNT
,OH.AVG_SLS_PER_DY_AMT
,OH.PROD_ASSAY_PCT
,OH.ORIG_CNTRY_ID
,OH.CURR_FYR
,OH.SLOC_ID
,OH.STORG_BIN_ID
,OH.PICK_AREA_FOR_LEAN_WM_TYP
,OH.PHY_INVTY_FYR
,OH.FIN_PROFIT_CNTR_ID
,OH.PHY_INVTY_BLOCK_TYP
,OH.VALUATED_STK_SLS_VAL_AMT
,OH.STK_TRNSFR_SLS_VAL_AMT
,OH.GL_PROD_COST_TYP
,OH.SUB_LOC_TYP
,OH.PAST_FYR_TM
,OH.PAST_FSCL_PERD_TM
,OH.PROD_ADD_LOC_YR_ID
,OH.PROD_REL_SET_ITEM,PLV.VALTN_AREA_LKEY,PLV.VALTN_TYP_LKEY,PLV.FYR_NBR,PLV.FMTH_NBR,PLV.STANDARD_PRICE,PLV.UNIT_PRICE,PLV.VALTN_CLASS,PLV.CO_KEY,PLV.CRNCY_KEY
FROM (SELECT START_DATE as EFF_DTE_RN,CASE WHEN LEAD(DATE(START_DATE)) over (partition by SRC_SYS_KEY,PROD_KEY,LOC_KEY,SLOC_ID order by START_DATE ASC) is NULL 
THEN '9999-12-31 00:00:00.000' ELSE LEAD(DATE(START_DATE)) over (partition by SRC_SYS_KEY,PROD_KEY,LOC_KEY,SLOC_ID order by START_DATE ASC) END  as EXPR_DTE_RN,*  FROM 
(SELECT  b.FMTH_BEGIN_DTE START_DATE,b.FMTH_END_DTE END_DATE,a.* FROM (SELECT * FROM {{source('CONSOLIDATED_PROD','VW_EDW_INVENTORY_ON_HAND_SNAPSHOT')}} 
WHERE  SRC_SYS_KEY IN ('SAPC11')) a 
LEFT JOIN (SELECT DISTINCT FMTH_BEGIN_DTE,FMTH_END_DTE,FMTH_ID FROM {{source('CONSOLIDATED_PROD','VW_EDW_CALENDAR')}}) b 
ON CASE WHEN (PAST_FYR_TM IS NOT NULL AND PAST_FSCL_PERD_TM IS NOT NULL) THEN CONCAT(a.PAST_FYR_TM,a.PAST_FSCL_PERD_TM) ELSE 
CONCAT(a.CURR_FYR,a.CURR_CLNDR_PERD_NBR) END =b.FMTH_ID WHERE a.SRC_SYS_KEY IN ('SAPC11') /* AND ETL_INS_PID  LIKE '%HIST%'*/) T) OH 
LEFT JOIN DIM_PRODUCT_LOC_VALUATION_SAPC11 PLV ON (CASE WHEN OH.ETL_INS_PID LIKE '%HIST%' THEN 
UPPER(concat(OH.PROD_KEY,'~',OH.LOC_KEY,'~',OH.PAST_FYR_TM,'~',OH.PAST_FSCL_PERD_TM)) ELSE UPPER(concat(OH.PROD_KEY,'~',OH.LOC_KEY,'~',OH.CURR_FYR,'~',OH.CURR_CLNDR_PERD_NBR)) END)= 
UPPER(concat(PLV.PROD_KEY,'~',PLV.VALTN_AREA_LKEY,'~',FYR_NBR,'~',PLV.FMTH_NBR)) AND OH.SRC_SYS_KEY = PLV.SRC_SYS_KEY
WHERE PLV.PROD_KEY iS NOT NULL AND PLV.VALTN_AREA_LKEY IS NOT NULL )
UNION ALL
(SELECT OH1.INVTY_ON_HAND_SNAPSHOT_KEY
,OH1.SRC_SYS_KEY
,OH1.SRC_RCRD_CREATE_DTE
,OH1.SRC_RCRD_CREATE_USERID
,OH1.SRC_RCRD_UPD_DTE
,OH1.SRC_RCRD_UPD_USERID
,OH1.RCRD_HASH_KEY
,OH1.DEL_FROM_SRC_FLAG
,OH1.EFF_DTE_RN 
,OH1.EXPR_DTE_RN 
,OH1.ETL_UPD_DTE 
,OH1.ETL_INS_PID 
,OH1.PROD_DFLT_LOC_KEY
,OH1.ZONE3_LOD_DTE
,OH1.PROD_KEY
,OH1.LOC_KEY
,OH1.RESTRCT_PROD_BY_LOC_TYP_LKEY
,OH1.INVTY_GRADE_LKEY
,OH1.INVTY_CRCTD_FACTOR_LKEY
,OH1.SLOC_MRP_LKEY
,OH1.INVTY_RESTRCT_USE_STK_STAT_LKEY
,OH1.INVTY_WHSE_USE_STK_STAT_LKEY
,OH1.INVTY_QLTY_INSPCT_STK_STAT_LKEY
,OH1.INVTY_BLKD_STK_STAT_LKEY
,OH1.PAST_INVTY_RESTRCT_USE_STK_STAT_LKEY
,OH1.PAST_INVTY_WHSE_USE_STK_STAT_LKEY
,OH1.PAST_INVTY_QLTY_INSPCT_STK_STAT_LKEY
,OH1.PAST_INVTY_BLKD_STK_STAT_LKEY
,OH1.SPCL_PROCUR_TYP_LKEY
,OH1.MTNC_STAT_TYP_LKEY
,OH1.CURR_PROD_COST_LKEY
,OH1.MRP_PROC_FLAG
,OH1.ROLL_FLAG
,OH1.SLOC_LVL_DEL_FLAG
,OH1.PAST_RCRD_EXISTS_FLAG
,OH1.PROD_RCV_DTE
,OH1.PROD_ADD_LOC_DTE
,OH1.LAST_STOCKROOM_ISS_DTE
,OH1.LAST_STOCKROOM_RCPT_DTE
,OH1.CYCLE_CNT_DTE
,OH1.SLS_AND_GOODS_ISS_CALC_DTE
,OH1.UNRSTR_USE_STK_LAST_CNT_DTE
,OH1.REF_WORK_ORD_NBR
,OH1.INVTY_LVL_NBR
,OH1.CURR_CLNDR_PERD_NBR
,OH1.PAST_RESTRCT_BATCHES_TOT_QTY
,OH1.PAST_INSPCT_STK_QTY
,OH1.PAST_VALUATED_STK_QTY
,OH1.PAST_BLKD_STK_RTRN_QTY
,OH1.PAST_BLKD_STK_QTY
,OH1.PAST_STK_IN_TRNSFR_QTY
,OH1.PROD_MAX_ORD_QTY
,OH1.PROD_ALLOC_QTY
,OH1.PROD_BKORD_QTY
,OH1.PROD_ON_HAND_QTY
,OH1.PROD_MIN_ORD_QTY
,OH1.PROD_NET_ON_HAND_QTY
,OH1.PROD_REQ_QTY
,OH1.PROD_ORD_QTY
,OH1.PROD_REORD_QTY
,OH1.PROD_NON_NETTABLE_ON_HAND_QTY
,OH1.ANTICIPATED_PROD_QTY
,OH1.RESTRCT_BATCHES_TOT_QTY
,OH1.QLTY_INSPCT_STK_QTY
,OH1.RESTRCT_USE_STK_QTY
,OH1.QLTY_INSPCT_CNSGN_STK_QTY
,OH1.UNRSTR_USE_STK_QTY
,OH1.BLKD_CNSGN_STK_QTY
,OH1.VALUATED_UNRSTR_STK_QTY
,OH1.REPL_QTY
,OH1.REORD_POINT_LVL_QTY
,OH1.BLKD_STK_RTRN_QTY
,OH1.BLKD_STK_QTY
,OH1.TRANSFERRED_STK_QTY
,OH1.PAST_PERD_RESTRCT_STK_QTY
,OH1.PAST_PERD_QLTY_INSPCT_QTY
,OH1.PAST_PERD_UNRSTR_USE_STK_QTY
,OH1.PAST_PERD_BLKD_STK_RTRN_QTY
,OH1.PAST_PERD_BLKD_STK_QTY
,OH1.PAST_PERD_TRNSFR_STK_QTY
,OH1.PAST_PROD_STK_VAL
,OH1.PAST_SLS_VAL
,OH1.DIV_ID
,OH1.PROD_RCV_YR_ID
,OH1.INVTY_STAT_TYP
,OH1.MISC_TYP
,OH1.TOT_SLS_RCPT_AMT
,OH1.TOT_SLS_ISS_AMT
,OH1.AVG_GOODS_ISS_PER_DY_CNT
,OH1.AVG_SLS_PER_DY_AMT
,OH1.PROD_ASSAY_PCT
,OH1.ORIG_CNTRY_ID
,OH1.CURR_FYR
,OH1.SLOC_ID
,OH1.STORG_BIN_ID
,OH1.PICK_AREA_FOR_LEAN_WM_TYP
,OH1.PHY_INVTY_FYR
,OH1.FIN_PROFIT_CNTR_ID
,OH1.PHY_INVTY_BLOCK_TYP
,OH1.VALUATED_STK_SLS_VAL_AMT
,OH1.STK_TRNSFR_SLS_VAL_AMT
,OH1.GL_PROD_COST_TYP
,OH1.SUB_LOC_TYP
,OH1.PAST_FYR_TM
,OH1.PAST_FSCL_PERD_TM
,OH1.PROD_ADD_LOC_YR_ID
,OH1.PROD_REL_SET_ITEM,PLV1.VALTN_AREA_LKEY,PLV1.VALTN_TYP_LKEY,PLV1.FYR_NBR,PLV1.FMTH_NBR,PLV1.STANDARD_PRICE,PLV1.UNIT_PRICE,PLV1.VALTN_CLASS,PLV1.CO_KEY,PLV1.CRNCY_KEY
 FROM
(SELECT OH.*
FROM ( SELECT START_DATE as EFF_DTE_RN,CASE WHEN LEAD(DATE(START_DATE)) over (partition by SRC_SYS_KEY,PROD_KEY,LOC_KEY,SLOC_ID order by START_DATE ASC) is NULL 
THEN '9999-12-31 00:00:00.000' ELSE LEAD(DATE(START_DATE)) over (partition by SRC_SYS_KEY,PROD_KEY,LOC_KEY,SLOC_ID order by START_DATE ASC) END  as EXPR_DTE_RN,*  FROM 
(SELECT  b.FMTH_BEGIN_DTE START_DATE,b.FMTH_END_DTE END_DATE,a.* FROM (SELECT * FROM {{source('CONSOLIDATED_PROD','VW_EDW_INVENTORY_ON_HAND_SNAPSHOT')}} 
WHERE  SRC_SYS_KEY IN ('SAPC11')) a 
LEFT JOIN (SELECT DISTINCT FMTH_BEGIN_DTE,FMTH_END_DTE,FMTH_ID FROM {{source('CONSOLIDATED_PROD','VW_EDW_CALENDAR')}}) b 
ON CASE WHEN (PAST_FYR_TM IS NOT NULL AND PAST_FSCL_PERD_TM IS NOT NULL) THEN CONCAT(a.PAST_FYR_TM,a.PAST_FSCL_PERD_TM) ELSE 
CONCAT(a.CURR_FYR,a.CURR_CLNDR_PERD_NBR) END  =b.FMTH_ID WHERE a.SRC_SYS_KEY IN ('SAPC11') /* AND ETL_INS_PID  LIKE '%HIST%'*/) T) OH 
LEFT JOIN DIM_PRODUCT_LOC_VALUATION_SAPC11 PLV ON  (CASE WHEN OH.ETL_INS_PID LIKE '%HIST%' THEN 
UPPER(concat(OH.PROD_KEY,'~',OH.LOC_KEY,'~',OH.PAST_FYR_TM,'~',OH.PAST_FSCL_PERD_TM)) ELSE UPPER(concat(OH.PROD_KEY,'~',OH.LOC_KEY,'~',OH.CURR_FYR,'~',OH.CURR_CLNDR_PERD_NBR)) END) = 
UPPER(concat(PLV.PROD_KEY,'~',PLV.VALTN_AREA_LKEY,'~',FYR_NBR,'~',PLV.FMTH_NBR)) AND OH.SRC_SYS_KEY = PLV.SRC_SYS_KEY 
WHERE PLV.PROD_KEY iS  NULL AND PLV.VALTN_AREA_LKEY IS NULL) OH1
LEFT JOIN DIM_PRODUCT_LOC_VALUATION_SAPC11 PLV1 ON 
UPPER(concat(OH1.PROD_KEY,'~',OH1.LOC_KEY))= UPPER(concat(PLV1.PROD_KEY,'~',PLV1.VALTN_AREA_LKEY)) AND 
CONCAT(PLV1.FYR_NBR,PLV1.FMTH_NBR)< CASE WHEN CONCAT(OH1.PAST_FYR_TM,OH1.PAST_FSCL_PERD_TM) IS NOT NULL THEN CONCAT(OH1.PAST_FYR_TM,OH1.PAST_FSCL_PERD_TM) 
ELSE CONCAT(OH1.CURR_FYR,OH1.CURR_CLNDR_PERD_NBR) END  AND OH1.SRC_SYS_KEY = PLV1.SRC_SYS_KEY 
QUALIFY ROW_NUMBER() OVER (PARTITION BY OH1.INVTY_ON_HAND_SNAPSHOT_KEY ,OH1.SRC_SYS_KEY,OH1.ZONE3_LOD_DTE  
ORDER BY CONCAT(PLV1.FYR_NBR,PLV1.FMTH_NBR) DESC) = 1)) OH