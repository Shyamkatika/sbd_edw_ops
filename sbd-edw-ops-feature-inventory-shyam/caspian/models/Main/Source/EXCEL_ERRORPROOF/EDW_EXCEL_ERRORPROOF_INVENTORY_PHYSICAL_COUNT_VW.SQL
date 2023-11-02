with PHYSCNT AS (
SELECT DISTINCT
  UPPER(TRIM(SITE_ID)) as SITE_ID,
  UPPER(TRIM(LOCATION_ID)) as LOCATION_ID ,  
UPPER(TRIM(PART_ID)) as PART_ID,
  UPPER(TRIM(COUNT_TYPE)) as COUNT_TYPE, 
CREATED_DATETIME,  
UPPER(TRIM(COUNTER_ID)) as COUNTER_ID ,
Counter_Name,
QUANTITY,
tag_id, 
audit_tag_id,
Zone_ID,
SUB_LOCATION_ID,
month(created_datetime),
LOADDTS
from {{source('EXCEL_ERRORPROOF','PHYSCNT')}}
),

SCT_DET AS (
SELECT DISTINCT
  UPPER(TRIM(SCT_SIM)) as SCT_SIM,
  UPPER(TRIM(SCT_PART)) as SCT_PART, 
   UPPER(TRIM(SCT_SITE)) as SCT_SITE,  
sct_cst_tot,
sct_mtl_tl, 
sct_mtl_ll
from {{source('EXCEL_QADDB','SCT_DET')}}
where UPPER(TRIM(SCT_DOMAIN))='EXCEL'  and  UPPER(TRIM(SCT_SIM))='STANDARD'
),

LD_DET AS (
SELECT DISTINCT
  UPPER(TRIM(LD_SITE)) as LD_SITE,
  UPPER(TRIM(LD_LOC)) as LD_LOC,
  UPPER(TRIM(LD_PART)) as LD_PART,
  UPPER(TRIM(LD_LOT)) as LD_LOT,
  UPPER(TRIM(LD_REF)) as LD_REF,
ld_date,
ld_qty_all,
ld_cnt_date,
ld_cust_consign_qty
from {{source('EXCEL_QADDB','LD_DET')}}
where UPPER(TRIM(LD_DOMAIN))='EXCEL'  
),

PT_MSTR AS(
SELECT 
  UPPER(TRIM(PT_SITE)) as PT_SITE,
  UPPER(TRIM(PT_LOC)) as PT_LOC,
  UPPER(TRIM(PT_PART)) as PT_PART, 
PT_DESC1,
PT_DESC2,
PT_PRICE,
PT_STATUS, 
  PT_PROD_LINE
from {{source('EXCEL_QADDB','PT_MSTR')}}
),

PL_MSTR as (
select PL_PROD_LINE 
from {{source('EXCEL_QADDB','PL_MSTR')}}
where UPPER(TRIM(PL_DOMAIN))='EXCEL'
),

PHYS_WG_INV as (
select distinct
UPPER(TRIM(Serial_ID)) as SERIAL_ID
,CREATED_DATETIME
,UPPER(TRIM(SITE_ID)) as SITE_ID
,UPPER(TRIM(COUNTER_ID)) as COUNTER_ID
,UPPER(TRIM(LOCATION_ID)) as LOCATION_ID
,LOADDTS
from {{source('EXCEL_ERRORPROOF','PHYS_WG_INV')}}
),

LABELS as (
select distinct IDENTITY_COLUMN, 
  UPPER(TRIM(Serial_ID)) as SERIAL_ID,
  UPPER(TRIM(PART_ID)) as PART_ID
  from {{source('EXCEL_ERRORPROOF','LABELS')}}
),

MAP_PHYSCNT as (
select * from  PHYSCNT
left join SCT_DET
on PHYSCNT.SITE_ID= SCT_DET.SCT_SITE  and PHYSCNT.PART_ID=SCT_DET.SCT_PART  
left join   LD_DET
on  PHYSCNT.PART_ID=LD_DET.LD_PART and PHYSCNT.SITE_ID= LD_DET.LD_SITE  and PHYSCNT.LOCATION_ID=LD_DET.LD_LOC
left join PT_MSTR 
  on PT_SITE=LD_SITE and PT_LOC =LD_LOC and PT_PART=LD_PART
  left join  PL_MSTR
  on PT_PROD_LINE=PL_PROD_LINE
),

MAP_PHYS_WG_INV as (
select distinct PHYS_WG_INV.SERIAL_ID, PHYS_WG_INV.CREATED_DATETIME, PHYS_WG_INV.SITE_ID, PHYS_WG_INV.COUNTER_ID, PHYS_WG_INV.LOCATION_ID, 
PHYS_WG_INV.LOADDTS, LABELS.IDENTITY_COLUMN,LABELS.PART_ID, LD_DET.LD_SITE, LD_DET.LD_LOC, LD_DET.LD_PART, LD_DET.LD_LOT, 
LD_DET.LD_REF, LD_DET.LD_DATE, LD_DET.LD_QTY_ALL, LD_DET.LD_CNT_DATE, LD_DET.LD_CUST_CONSIGN_QTY, PT_MSTR.PT_SITE,
 PT_MSTR.PT_LOC, PT_MSTR.PT_PART, PT_MSTR.PT_DESC1, PT_MSTR.PT_DESC2, PT_MSTR.PT_PRICE,PT_MSTR.PT_STATUS,PT_MSTR.PT_PROD_LINE 
 from  PHYS_WG_INV
  left join   LABELS
  on PHYS_WG_INV.SERIAL_ID=LABELS.SERIAL_ID
  LEFT JOIN   PT_MSTR
  on PT_PART=LABELS.PART_ID
  LEFT JOIN  LD_DET
  on   LD_DET.LD_PART=LABELS.PART_ID
and  LD_DET.LD_LOT=LABELS.SERIAL_ID
),

MAP_OUT_PHYSCNT  as (
select 
upper(concat(coalesce(trim(COUNT_TYPE)::varchar, ''),'~',coalesce(trim(SITE_ID)::varchar, ''),'~',coalesce(trim(LOCATION_ID)::varchar, '')
,'~',coalesce(trim(PART_ID)::varchar, ''),'~',coalesce(trim(CREATED_DATETIME)::varchar, ''),'~',coalesce(trim(COUNTER_ID)::varchar, ''))) as PHY_INVTY_KEY,
'EXCEL_ERRORPROOF' AS {{var('column_srcsyskey')}},
md5(upper(concat(coalesce(trim(COUNT_TYPE)::varchar, ''),'~',coalesce(trim(SITE_ID)::varchar, ''),'~',coalesce(trim(LOCATION_ID)::varchar, '')
,'~',coalesce(trim(PART_ID)::varchar, ''),'~',coalesce(trim(CREATED_DATETIME)::varchar, ''),'~',coalesce(trim(COUNTER_ID)::varchar, '')))) AS {{var('column_rechashkey')}},
CREATED_DATETIME  AS {{var('column_SRC_RCRD_CREATE_DTE')}},
COUNTER_ID AS {{var('column_SRC_RCRD_CREATE_USERID')}},
LD_DATE  AS {{var('column_SRC_RCRD_UPD_DTE')}},
NULL  AS {{var('column_SRC_RCRD_UPD_USERID')}},
{{var('default_n')}} AS  {{var('column_DEL_FROM_SRC_FLAG')}},                                                                               
'{{model.name}}' AS  {{var('column_ETL_INS_PID')}},                                                                  
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS  {{var('column_ETL_UPD_PID')}}, 
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_UPD_DTE')}},
MAP_PHYSCNT.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}}, 
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
MAP_PHYSCNT.LOADDTS AS {{var('column_z3loddtm')}},
PL_PROD_LINE as GRP_CRITERION_TYP,
SITE_ID As LOC_KEY,
'' as PROD_BATCH_NBR,
PART_ID as PROD_KEY	,
COUNTER_ID as ADJ_POST_BY_USERID,
COUNTER_NAME as COUNTED_BY_USERID,
CASE WHEN PT_STATUS = 'OB' THEN 'Y' ELSE 'N' END as ITEM_DEL_FLAG,
CASE WHEN LENGTH(TRIM(audit_tag_id)) = 0 THEN 'N' ELSE 'Y' END AS ZERO_CNT_FLAG,
CASE WHEN LENGTH(TRIM(audit_tag_id)) = 0 THEN '' ELSE UPPER(TRIM(audit_tag_id)) END as PHY_INVTY_CNT_NBR,
PART_ID as CNT_PROD_KEY,
concat(LOCATION_ID,SUB_LOCATION_ID) as  CNT_SUB_LOC_KEY	,
CASE WHEN LENGTH(TRIM(audit_tag_id)) = 0 THEN 0 ELSE QUANTITY END as CNT_QTY,
CASE  WHEN LENGTH(TRIM(audit_tag_id)) = 0 THEN '1900-01-01' ELSE created_datetime END as CNT_DTE,
CASE WHEN LENGTH(TRIM(audit_tag_id)) = 0 THEN 0 ELSE QUANTITY END as RECOUNT_QTY,
CASE WHEN LENGTH(TRIM(audit_tag_id)) = 0 THEN '1900-01-01' ELSE created_datetime END as RECOUNT_DTE,
SITE_ID AS TAG_LOC_KEY,
CREATED_DATETIME AS CNT_CREATE_DTE,
PART_ID AS INVTY_PROD_KEY,
PT_PRICE AS PROD_UNIT_PRC_AMT,
CREATED_DATETIME AS CNT_TAG_CREATE_DTE,
PART_ID AS PHY_INVTY_PROD_KEY,
LOCATION_ID AS LOC_CD_LKEY,
SCT_CST_TOT AS UNIT_COST,
CREATED_DATETIME AS CNT_ENTR_DTE,
'' AS ORG_KEY,
ZONE_ID AS PLANT_LOC_KEY,
PART_ID AS PROD_1_KEY,
CASE WHEN LENGTH(TRIM(audit_tag_id)) = 0 THEN '' ELSE UPPER(TRIM(tag_id)) END AS SET_NBR,
'' AS PROD_GRP_TYP_LKEY	,
LOCATION_ID AS WHSE_LOC_LKEY,
0 AS SLOC_CNT,
QUANTITY AS PROD_ON_HAND_QTY,
LD_QTY_ALL AS PROD_ALLOC_ORD_QTY,
LD_CNT_DATE AS LAST_CNT_DTE,
CASE WHEN UPPER(TRIM(sct_sim)) = 'CURRENT' THEN sct_mtl_tl + sct_mtl_ll ELSE 0 END AS PROD_MATL_COST,
sct_mtl_tl AS PROD_LABR_COST,
CASE WHEN UPPER(TRIM(sct_sim)) = 'CURRENT' THEN sct_mtl_tl + sct_mtl_ll ELSE 0 END AS PROD_MATL_CURR_COST,
CASE WHEN UPPER(TRIM(sct_sim)) = 'CURRENT' THEN sct_mtl_tl + sct_mtl_ll ELSE 0 END AS PROD_LABR_CURR_COST,
LD_CUST_CONSIGN_QTY AS MIN_CNSGN_QTY,
CASE WHEN LENGTH(TRIM(audit_tag_id)) = 0 THEN '' ELSE audit_tag_id END as PROD_TAG_NBR,
QUANTITY AS PHY_INVTY_QTY,
CONCAT(PT_DESC1,PT_DESC2) AS PROD_DESC,
SUBSTR(CAST(CREATED_DATETIME AS VARCHAR),6,2) AS DOC_CREATE_NBR,
SUB_LOCATION_ID AS STORG_BIN_NBR,
SUB_LOCATION_ID AS SUB_SLOC_ID	
from MAP_PHYSCNT
),

MAP_OUT_PHYS_WG_INV as(
SELECT
upper(concat(coalesce(trim(SERIAL_ID)::varchar, ''),'~',coalesce(trim(SITE_ID)::varchar, ''),'~',coalesce(trim(LOCATION_ID)::varchar, '')
,'~',coalesce(trim(PART_ID)::varchar, ''),'~',coalesce(trim(COUNTER_ID)::varchar, ''),'~',coalesce(trim(CREATED_DATETIME)::varchar, ''))) as PHY_INVTY_KEY,
'EXCEL_ERRORPROOF' AS {{var('column_srcsyskey')}},
md5(upper(concat(coalesce(trim(SERIAL_ID)::varchar, ''),'~',coalesce(trim(SITE_ID)::varchar, ''),'~',coalesce(trim(LOCATION_ID)::varchar, '')
,'~',coalesce(trim(PART_ID)::varchar, ''),'~',coalesce(trim(COUNTER_ID)::varchar, ''),'~',coalesce(trim(CREATED_DATETIME)::varchar, '')))) AS {{var('column_rechashkey')}},
CREATED_DATETIME  AS {{var('column_SRC_RCRD_CREATE_DTE')}},
NULL AS {{var('column_SRC_RCRD_CREATE_USERID')}},
 '1900-01-01' AS {{var('column_SRC_RCRD_UPD_DTE')}},
NULL  AS {{var('column_SRC_RCRD_UPD_USERID')}},
{{var('default_n')}} AS  {{var('column_DEL_FROM_SRC_FLAG')}},                                                                               
'{{model.name}}' AS  {{var('column_ETL_INS_PID')}},                                                                  
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS  {{var('column_ETL_UPD_PID')}}, 
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_UPD_DTE')}},
MAP_PHYS_WG_INV.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}}, 
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
MAP_PHYS_WG_INV.LOADDTS AS {{var('column_z3loddtm')}},
'' as GRP_CRITERION_TYP,
SITE_ID As LOC_KEY,
SERIAL_ID as PROD_BATCH_NBR,
PART_ID as PROD_KEY,
COUNTER_ID as ADJ_POST_BY_USERID,
COUNTER_ID as COUNTED_BY_USERID,
'' as ITEM_DEL_FLAG,
'' as ZERO_CNT_FLAG,
'0'  as PHY_INVTY_CNT_NBR,
'' as CNT_PROD_KEY,
LOCATION_ID as  CNT_SUB_LOC_KEY,
0 as CNT_QTY,
CREATED_DATETIME as CNT_DTE,
0 as RECOUNT_QTY,
'1900-01-01' as RECOUNT_DTE,
SITE_ID AS TAG_LOC_KEY,
CREATED_DATETIME AS CNT_CREATE_DTE,
'' AS INVTY_PROD_KEY,
PT_PRICE AS PROD_UNIT_PRC_AMT,
'1900-01-01' AS CNT_TAG_CREATE_DTE,
'' AS PHY_INVTY_PROD_KEY,
LOCATION_ID AS LOC_CD_LKEY,
0 AS UNIT_COST,
CREATED_DATETIME AS CNT_ENTR_DTE,
'' AS ORG_KEY,
'' AS PLANT_LOC_KEY,
'' AS PROD_1_KEY,
'' AS SET_NBR,
'' AS PROD_GRP_TYP_LKEY,
LOCATION_ID as WHSE_LOC_LKEY,
0 AS SLOC_CNT,
LD_QTY_ALL AS PROD_ON_HAND_QTY,
LD_QTY_ALL AS PROD_ALLOC_ORD_QTY,
LD_CNT_DATE AS LAST_CNT_DTE,
0 AS PROD_MATL_COST,
0 AS PROD_LABR_COST,
0 AS PROD_MATL_CURR_COST,
0 AS PROD_LABR_CURR_COST,
LD_CUST_CONSIGN_QTY AS MIN_CNSGN_QTY,
'' AS PROD_TAG_NBR,
0 AS PHY_INVTY_QTY,
CONCAT(PT_DESC1,PT_DESC2) AS PROD_DESC,
'' AS DOC_CREATE_NBR,
'' AS STORG_BIN_NBR,
'' AS SUB_SLOC_ID	
from MAP_PHYS_WG_INV
),

CHECK_UNION AS (
select distinct * from MAP_OUT_PHYSCNT
UNION ALL 
SELECT distinct * FROM MAP_OUT_PHYS_WG_INV )

SELECT * FROM CHECK_UNION
