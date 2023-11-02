with 
IMHISTAUS AS(
select upper(concat(coalesce(trim(IMHISTA.ITNBR)::varchar, ''),'~',
                    coalesce(trim(IMHISTA.HOUSE)::varchar, ''),'~',
                    coalesce(trim(IMHISTA.TAID)::varchar, ''),'~',
                    coalesce(trim(IMHISTA.TCODE)::varchar, ''),'~',
                    coalesce(trim(IMHISTA.PSTMAV)::varchar, ''),'~','IMHIST_AUS')) as INVTY_MVMNT_HDR_KEY,
'INFORXA' as SRC_SYS_KEY,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTA.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTA.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTA.UPDDT), 7, 0), 6, 2)))) as SRC_RCRD_CREATE_DTE,
null as SRC_RCRD_CREATE_USERID,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTA.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTA.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTA.UPDDT), 7, 0), 6, 2)))) as SRC_RCRD_UPD_DTE,
null as SRC_RCRD_UPD_USERID,
md5(upper(concat(coalesce(trim(IMHISTA.ITNBR)::varchar, ''),'~',
                    coalesce(trim(IMHISTA.HOUSE)::varchar, ''),'~',
                    coalesce(trim(IMHISTA.TAID)::varchar, ''),'~',
                    coalesce(trim(IMHISTA.TCODE)::varchar, ''),'~',
                    coalesce(trim(IMHISTA.PSTMAV)::varchar, ''),'~','IMHIST_AUS'))) as RCRD_HASH_KEY, --
'N' as DEL_FROM_SRC_FLAG,
'EDW_INFORXA_INVENTORY_MOVEMENT_HEADER_VW' as ETL_INS_PID,
current_timestamp::timestamp_ntz as ETL_INS_DTE,
'EDW_INFORXA_INVENTORY_MOVEMENT_HEADER_VW' as ETL_UPD_PID,
current_timestamp::timestamp_ntz as ETL_UPD_DTE,
IMHISTA.loaddts as EFF_DTE,
'9999-12-31 00:00:00.000' as EXPR_DTE,
'Y' as CURR_RCRD_FLAG,
'N' as ORP_RCRD_FLAG,
IMHISTA.loaddts as ZONE3_LOD_DTE,
--IMHIST.ACREC AS CURR_RCRD_FLAG,
IMHISTA.REFNO AS PROD_MVMNT_DOC_NBR,
IMHISTA.TCODE AS MVMNT_TYP_LKEY,
 ITMPLN_AUS.MINQ AS PROD_MAX_ORD_QTY,
 ITMPLN_AUS.MAXQ AS PROD_MIN_ORD_QTY,
ITMPLN_AUS.MULQ AS PROD_LOT_ORD_QTY,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTA.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTA.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTA.UPDDT), 7, 0), 6, 2)))) as PROD_MVMNT_DOC_DTE,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTA.TRNDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTA.TRNDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTA.TRNDT), 7, 0), 6, 2)))) as PROD_MVMNT_POST_DTE,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTA.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTA.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTA.UPDDT), 7, 0), 6, 2)))) as PROD_MVMNT_DOC_ENTR_DTE,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTA.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTA.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTA.UPDDT), 7, 0), 6, 2)))) as PROD_MVMNT_DOC_CHG_DTE,
IMHISTA.LGWNO AS GOODS_RCPT_NBR,
IMHISTA.ITNBR AS PROD_KEY,
CONCAT('AUS','~',IMHISTA.HOUSE) AS LOC_KEY,
IMHISTA.PLANN AS PROD_RESP_PRSN_NAME
FROM {{source('INFORXA','IMHISTAUS')}} IMHISTA
LEFT JOIN {{source('INFORXA','ITMPLN_AUS')}} ITMPLN_AUS
ON ITMPLN_AUS.ITNB = IMHISTA.ITNBR
AND ITMPLN_AUS.WHID = IMHISTA.HOUSE

),
--
-- IMHISTBRN AS(
-- select upper(concat(coalesce(trim(IMHISTB.ITNBR)::varchar, ''),'~',
--                     coalesce(trim(IMHISTB.HOUSE)::varchar, ''),'~',
--                     coalesce(trim(IMHISTB.TAID)::varchar, ''),'~',
--                     coalesce(trim(IMHISTB.TCODE)::varchar, ''),'~',
--                     coalesce(trim(IMHISTB.PSTMAV)::varchar, ''),'~','IMHIST_BRN')) as INVTY_MVMNT_HDR_KEY,
-- 'INFORXA' as SRC_SYS_KEY,
-- dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTB.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTB.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTB.UPDDT), 7, 0), 6, 2)))) as SRC_RCRD_CREATE_DTE,
-- null as SRC_RCRD_CREATE_USERID,
-- dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTB.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTB.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTB.UPDDT), 7, 0), 6, 2)))) as SRC_RCRD_UPD_DTE,
-- null as SRC_RCRD_UPD_USERID,
-- md5(upper(concat(coalesce(trim(IMHISTB.ITNBR)::varchar, ''),'~',
--                     coalesce(trim(IMHISTB.HOUSE)::varchar, ''),'~',
--                     coalesce(trim(IMHISTB.TAID)::varchar, ''),'~',
--                     coalesce(trim(IMHISTB.TCODE)::varchar, ''),'~',
--                     coalesce(trim(IMHISTB.PSTMAV)::varchar, ''),'~','IMHIST_BRN'))) as RCRD_HASH_KEY,
-- 'N' as DEL_FROM_SRC_FLAG,
-- 'EDW_INFORXA_INVENTORY_MOVEMENT_HEADER_VW' as ETL_INS_PID,
-- current_timestamp::timestamp_ntz as ETL_INS_DTE,
-- 'EDW_INFORXA_INVENTORY_MOVEMENT_HEADER_VW' as ETL_UPD_PID,
-- current_timestamp::timestamp_ntz as ETL_UPD_DTE,
-- IMHISTB.loaddts as EFF_DTE,
-- '9999-12-31 00:00:00.000' as EXPR_DTE,
-- 'Y' as CURR_RCRD_FLAG,
-- 'N' as ORP_RCRD_FLAG,
-- IMHISTB.loaddts as ZONE3_LOD_DTE,
-- --IMHIST.ACREC AS CURR_RCRD_FLAG,
-- IMHISTB.REFNO AS PROD_MVMNT_DOC_NBR,
-- IMHISTB.TCODE AS MVMNT_TYP_LKEY,
-- NULL AS PROD_MAX_ORD_QTY,
--  NULL AS PROD_MIN_ORD_QTY,
-- NULL AS PROD_LOT_ORD_QTY,
-- dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTB.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTB.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTB.UPDDT), 7, 0), 6, 2)))) as PROD_MVMNT_DOC_DTE,
-- dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTB.TRNDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTB.TRNDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTB.TRNDT), 7, 0), 6, 2)))) as PROD_MVMNT_POST_DTE,
-- dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTB.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTB.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTB.UPDDT), 7, 0), 6, 2)))) as PROD_MVMNT_DOC_ENTR_DTE,
-- dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTB.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTB.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTB.UPDDT), 7, 0), 6, 2)))) as PROD_MVMNT_DOC_CHG_DTE,
-- IMHISTB.LGWNO AS GOODS_RCPT_NBR,
-- IMHISTB.ITNBR AS PROD_KEY,
-- CONCAT('BRN','~',IMHISTB.HOUSE),
-- IMHISTB.PLANN AS PROD_RESP_PRSN_NAME
-- FROM {{source('INFORXA','IMHISTBRN')}} IMHISTB
-- ),
--###################################################################################
IMHISTCANS AS(
select upper(concat(coalesce(trim(IMHISTC.ITNBR)::varchar, ''),'~',
                    coalesce(trim(IMHISTC.HOUSE)::varchar, ''),'~',
                    coalesce(trim(IMHISTC.TAID)::varchar, ''),'~',
                    coalesce(trim(IMHISTC.TCODE)::varchar, ''),'~',
                    coalesce(trim(IMHISTC.PSTMAV)::varchar, ''),'~','IMHIST_CANS')) as INVTY_MVMNT_HDR_KEY,
'INFORXA' as SRC_SYS_KEY,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTC.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTC.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTC.UPDDT), 7, 0), 6, 2)))) as SRC_RCRD_CREATE_DTE,
null as SRC_RCRD_CREATE_USERID,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTC.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTC.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTC.UPDDT), 7, 0), 6, 2)))) as SRC_RCRD_UPD_DTE,
null as SRC_RCRD_UPD_USERID,
md5(upper(concat(coalesce(trim(IMHISTC.ITNBR)::varchar, ''),'~',
                    coalesce(trim(IMHISTC.HOUSE)::varchar, ''),'~',
                    coalesce(trim(IMHISTC.TAID)::varchar, ''),'~',
                    coalesce(trim(IMHISTC.TCODE)::varchar, ''),'~',
                    coalesce(trim(IMHISTC.PSTMAV)::varchar, ''),'~','IMHIST_CANS'))) as RCRD_HASH_KEY,
'N' as DEL_FROM_SRC_FLAG,
'EDW_INFORXA_INVENTORY_MOVEMENT_HEADER_VW' as ETL_INS_PID,
current_timestamp::timestamp_ntz as ETL_INS_DTE,
'EDW_INFORXA_INVENTORY_MOVEMENT_HEADER_VW' as ETL_UPD_PID,
current_timestamp::timestamp_ntz as ETL_UPD_DTE,
IMHISTC.loaddts as EFF_DTE,
'9999-12-31 00:00:00.000' as EXPR_DTE,
'Y' as CURR_RCRD_FLAG,
'N' as ORP_RCRD_FLAG,
IMHISTC.loaddts as ZONE3_LOD_DTE,
--IMHIST.ACREC AS CURR_RCRD_FLAG,
IMHISTC.REFNO AS PROD_MVMNT_DOC_NBR,
IMHISTC.TCODE AS MVMNT_TYP_LKEY,
 ITMPLNC.MINQ AS PROD_MAX_ORD_QTY,
 ITMPLNC.MAXQ AS PROD_MIN_ORD_QTY,
ITMPLNC.MULQ AS PROD_LOT_ORD_QTY,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTC.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTC.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTC.UPDDT), 7, 0), 6, 2)))) as PROD_MVMNT_DOC_DTE,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTC.TRNDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTC.TRNDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTC.TRNDT), 7, 0), 6, 2)))) as PROD_MVMNT_POST_DTE,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTC.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTC.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTC.UPDDT), 7, 0), 6, 2)))) as PROD_MVMNT_DOC_ENTR_DTE,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTC.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTC.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTC.UPDDT), 7, 0), 6, 2)))) as PROD_MVMNT_DOC_CHG_DTE,
IMHISTC.LGWNO AS GOODS_RCPT_NBR,
IMHISTC.ITNBR AS PROD_KEY,
CONCAT('CANS','~',IMHISTC.HOUSE) AS LOC_KEY,
IMHISTC.PLANN AS PROD_RESP_PRSN_NAME
FROM {{source('INFORXA','IMHISTCANS')}} IMHISTC
LEFT JOIN {{source('INFORXA','ITMPLN_CANSVC')}} ITMPLNC
ON ITMPLNC.ITNB = IMHISTC.ITNBR
AND ITMPLNC.WHID = IMHISTC.HOUSE
),
-- --###################################################################################
IMHISTCANW AS(
select upper(concat(coalesce(trim(IMHISTD.ITNBR)::varchar, ''),'~',
                    coalesce(trim(IMHISTD.HOUSE)::varchar, ''),'~',
                    coalesce(trim(IMHISTD.TAID)::varchar, ''),'~',
                    coalesce(trim(IMHISTD.TCODE)::varchar, ''),'~',
                    coalesce(trim(IMHISTD.PSTMAV)::varchar, ''),'~','IMHIST_CANW')) as INVTY_MVMNT_HDR_KEY,
'INFORXA' as SRC_SYS_KEY,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTD.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTD.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTD.UPDDT), 7, 0), 6, 2)))) as SRC_RCRD_CREATE_DTE,
null as SRC_RCRD_CREATE_USERID,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTD.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTD.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTD.UPDDT), 7, 0), 6, 2)))) as SRC_RCRD_UPD_DTE,
null as SRC_RCRD_UPD_USERID,
md5(upper(concat(coalesce(trim(IMHISTD.ITNBR)::varchar, ''),'~',
                    coalesce(trim(IMHISTD.HOUSE)::varchar, ''),'~',
                    coalesce(trim(IMHISTD.TAID)::varchar, ''),'~',
                    coalesce(trim(IMHISTD.TCODE)::varchar, ''),'~',
                    coalesce(trim(IMHISTD.PSTMAV)::varchar, ''),'~','IMHIST_CANW'))) as RCRD_HASH_KEY,
'N' as DEL_FROM_SRC_FLAG,
'EDW_INFORXA_INVENTORY_MOVEMENT_HEADER_VW' as ETL_INS_PID,
current_timestamp::timestamp_ntz as ETL_INS_DTE,
'EDW_INFORXA_INVENTORY_MOVEMENT_HEADER_VW' as ETL_UPD_PID,
current_timestamp::timestamp_ntz as ETL_UPD_DTE,
IMHISTD.loaddts as EFF_DTE,
'9999-12-31 00:00:00.000' as EXPR_DTE,
'Y' as CURR_RCRD_FLAG,
'N' as ORP_RCRD_FLAG,
IMHISTD.loaddts as ZONE3_LOD_DTE,
--IMHIST.ACREC AS CURR_RCRD_FLAG,
IMHISTD.REFNO AS PROD_MVMNT_DOC_NBR,
IMHISTD.TCODE AS MVMNT_TYP_LKEY,
 ITMPLND.MINQ AS PROD_MAX_ORD_QTY,
 ITMPLND.MAXQ AS PROD_MIN_ORD_QTY,
ITMPLND.MULQ AS PROD_LOT_ORD_QTY,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTD.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTD.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTD.UPDDT), 7, 0), 6, 2)))) as PROD_MVMNT_DOC_DTE,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTD.TRNDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTD.TRNDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTD.TRNDT), 7, 0), 6, 2)))) as PROD_MVMNT_POST_DTE,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTD.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTD.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTD.UPDDT), 7, 0), 6, 2)))) as PROD_MVMNT_DOC_ENTR_DTE,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTD.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTD.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTD.UPDDT), 7, 0), 6, 2)))) as PROD_MVMNT_DOC_CHG_DTE,
IMHISTD.LGWNO AS GOODS_RCPT_NBR,
IMHISTD.ITNBR AS PROD_KEY,
CONCAT('CANW','~',IMHISTD.HOUSE),
IMHISTD.PLANN AS PROD_RESP_PRSN_NAME
FROM {{source('INFORXA','IMHISTCANW')}} IMHISTD
LEFT JOIN {{source('INFORXA','ITMPLN_CANWG')}} ITMPLND
ON ITMPLND.ITNB = IMHISTD.ITNBR
AND ITMPLND.WHID = IMHISTD.HOUSE

),
-- --#######################################################################
IMHISTCHI AS(
select upper(concat(coalesce(trim(IMHISTE.ITNBR)::varchar, ''),'~',
                    coalesce(trim(IMHISTE.HOUSE)::varchar, ''),'~',
                    coalesce(trim(IMHISTE.TAID)::varchar, ''),'~',
                    coalesce(trim(IMHISTE.TCODE)::varchar, ''),'~',
                    coalesce(trim(IMHISTE.PSTMAV)::varchar, ''),'~','IMHIST_CHI')) as INVTY_MVMNT_HDR_KEY,
'INFORXA' as SRC_SYS_KEY,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTE.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTE.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTE.UPDDT), 7, 0), 6, 2)))) as SRC_RCRD_CREATE_DTE,
null as SRC_RCRD_CREATE_USERID,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTE.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTE.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTE.UPDDT), 7, 0), 6, 2)))) as SRC_RCRD_UPD_DTE,
null as SRC_RCRD_UPD_USERID,
md5(upper(concat(coalesce(trim(IMHISTE.ITNBR)::varchar, ''),'~',
                    coalesce(trim(IMHISTE.HOUSE)::varchar, ''),'~',
                    coalesce(trim(IMHISTE.TAID)::varchar, ''),'~',
                    coalesce(trim(IMHISTE.TCODE)::varchar, ''),'~',
                    coalesce(trim(IMHISTE.PSTMAV)::varchar, ''),'~','IMHIST_CHI'))) as RCRD_HASH_KEY,
'N' as DEL_FROM_SRC_FLAG,
'EDW_INFORXA_INVENTORY_MOVEMENT_HEADER_VW' as ETL_INS_PID,
current_timestamp::timestamp_ntz as ETL_INS_DTE,
'EDW_INFORXA_INVENTORY_MOVEMENT_HEADER_VW' as ETL_UPD_PID,
current_timestamp::timestamp_ntz as ETL_UPD_DTE,
IMHISTE.loaddts as EFF_DTE,
'9999-12-31 00:00:00.000' as EXPR_DTE,
'Y' as CURR_RCRD_FLAG,
'N' as ORP_RCRD_FLAG,
IMHISTE.loaddts as ZONE3_LOD_DTE,
--IMHIST.ACREC AS CURR_RCRD_FLAG,
IMHISTE.REFNO AS PROD_MVMNT_DOC_NBR,
IMHISTE.TCODE AS MVMNT_TYP_LKEY,
 ITMPLNE.MINQ AS PROD_MAX_ORD_QTY,
 ITMPLNE.MAXQ AS PROD_MIN_ORD_QTY,
ITMPLNE.MULQ AS PROD_LOT_ORD_QTY,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTE.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTE.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTE.UPDDT), 7, 0), 6, 2)))) as PROD_MVMNT_DOC_DTE,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTE.TRNDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTE.TRNDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTE.TRNDT), 7, 0), 6, 2)))) as PROD_MVMNT_POST_DTE,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTE.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTE.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTE.UPDDT), 7, 0), 6, 2)))) as PROD_MVMNT_DOC_ENTR_DTE,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTE.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTE.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTE.UPDDT), 7, 0), 6, 2)))) as PROD_MVMNT_DOC_CHG_DTE,
IMHISTE.LGWNO AS GOODS_RCPT_NBR,
IMHISTE.ITNBR AS PROD_KEY,
CONCAT('CHI','~',IMHISTE.HOUSE) AS LOC_KEY,
IMHISTE.PLANN AS PROD_RESP_PRSN_NAME
FROM {{source('INFORXA','IMHISTCHI')}} IMHISTE
LEFT JOIN {{source('INFORXA','ITMPLN_CHI')}} ITMPLNE
ON ITMPLNE.ITNB = IMHISTE.ITNBR
AND ITMPLNE.WHID = IMHISTE.HOUSE
),
--###############################################################################
-- IMHISTLEI AS(
-- select upper(concat(coalesce(trim(IMHISTF.ITNBR)::varchar, ''),'~',
--                     coalesce(trim(IMHISTF.HOUSE)::varchar, ''),'~',
--                     coalesce(trim(IMHISTF.TAID)::varchar, ''),'~',
--                     coalesce(trim(IMHISTF.TCODE)::varchar, ''),'~',
--                     coalesce(trim(IMHISTF.PSTMAV)::varchar, ''),'~','IMHIST_LEI')) as INVTY_MVMNT_HDR_KEY,
-- 'INFORXA' as SRC_SYS_KEY,
-- dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTF.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTF.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTF.UPDDT), 7, 0), 6, 2)))) as SRC_RCRD_CREATE_DTE,
-- null as SRC_RCRD_CREATE_USERID,
-- dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTF.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTF.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTF.UPDDT), 7, 0), 6, 2)))) as SRC_RCRD_UPD_DTE,
-- null as SRC_RCRD_UPD_USERID,
-- md5(upper(concat(coalesce(trim(IMHISTF.ITNBR)::varchar, ''),'~',
--                     coalesce(trim(IMHISTF.HOUSE)::varchar, ''),'~',
--                     coalesce(trim(IMHISTF.TAID)::varchar, ''),'~',
--                     coalesce(trim(IMHISTF.TCODE)::varchar, ''),'~',
--                     coalesce(trim(IMHISTF.PSTMAV)::varchar, ''),'~','IMHIST_LEI'))) as RCRD_HASH_KEY,
-- 'N' as DEL_FROM_SRC_FLAG,
-- 'EDW_INFORXA_INVENTORY_MOVEMENT_HEADER_VW' as ETL_INS_PID,
-- current_timestamp::timestamp_ntz as ETL_INS_DTE,
-- 'EDW_INFORXA_INVENTORY_MOVEMENT_HEADER_VW' as ETL_UPD_PID,
-- current_timestamp::timestamp_ntz as ETL_UPD_DTE,
-- IMHISTF.loaddts as EFF_DTE,
-- '9999-12-31 00:00:00.000' as EXPR_DTE,
-- 'Y' as CURR_RCRD_FLAG,
-- 'N' as ORP_RCRD_FLAG,
-- IMHISTF.loaddts as ZONE3_LOD_DTE,
-- --IMHIST.ACREC AS CURR_RCRD_FLAG,
-- IMHISTF.REFNO AS PROD_MVMNT_DOC_NBR,
-- IMHISTF.TCODE AS MVMNT_TYP_LKEY,
--  NULL AS PROD_MAX_ORD_QTY,
--  NULL AS PROD_MIN_ORD_QTY,
-- NULL AS PROD_LOT_ORD_QTY,
-- dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTF.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTF.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTF.UPDDT), 7, 0), 6, 2)))) as PROD_MVMNT_DOC_DTE,
-- dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTF.TRNDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTF.TRNDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTF.TRNDT), 7, 0), 6, 2)))) as PROD_MVMNT_POST_DTE,
-- dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTF.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTF.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTF.UPDDT), 7, 0), 6, 2)))) as PROD_MVMNT_DOC_ENTR_DTE,
-- dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTF.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTF.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTF.UPDDT), 7, 0), 6, 2)))) as PROD_MVMNT_DOC_CHG_DTE,
-- IMHISTF.LGWNO AS GOODS_RCPT_NBR,
-- IMHISTF.ITNBR AS PROD_KEY,
-- CONCAT('LEI','~',IMHISTF.HOUSE) AS LOC_KEY,
-- IMHISTF.PLANN AS PROD_RESP_PRSN_NAME
-- FROM {{source('INFORXA','IMHISTLEI')}} IMHISTF),
--###############################################################################
IMHISTMAR AS(
select upper(concat(coalesce(trim(IMHISTG.ITNBR)::varchar, ''),'~',
                    coalesce(trim(IMHISTG.HOUSE)::varchar, ''),'~',
                    coalesce(trim(IMHISTG.TAID)::varchar, ''),'~',
                    coalesce(trim(IMHISTG.TCODE)::varchar, ''),'~',
                    coalesce(trim(IMHISTG.PSTMAV)::varchar, ''),'~','IMHIST_MAR')) as INVTY_MVMNT_HDR_KEY,
'INFORXA' as SRC_SYS_KEY,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTG.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTG.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTG.UPDDT), 7, 0), 6, 2)))) as SRC_RCRD_CREATE_DTE,
null as SRC_RCRD_CREATE_USERID,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTG.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTG.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTG.UPDDT), 7, 0), 6, 2)))) as SRC_RCRD_UPD_DTE,
null as SRC_RCRD_UPD_USERID,
md5(upper(concat(coalesce(trim(IMHISTG.ITNBR)::varchar, ''),'~',
                    coalesce(trim(IMHISTG.HOUSE)::varchar, ''),'~',
                    coalesce(trim(IMHISTG.TAID)::varchar, ''),'~',
                    coalesce(trim(IMHISTG.TCODE)::varchar, ''),'~',
                    coalesce(trim(IMHISTG.PSTMAV)::varchar, ''),'~','IMHIST_MAR'))) as RCRD_HASH_KEY,
'N' as DEL_FROM_SRC_FLAG,
'EDW_INFORXA_INVENTORY_MOVEMENT_HEADER_VW' as ETL_INS_PID,
current_timestamp::timestamp_ntz as ETL_INS_DTE,
'EDW_INFORXA_INVENTORY_MOVEMENT_HEADER_VW' as ETL_UPD_PID,
current_timestamp::timestamp_ntz as ETL_UPD_DTE,
IMHISTG.loaddts as EFF_DTE,
'9999-12-31 00:00:00.000' as EXPR_DTE,
'Y' as CURR_RCRD_FLAG,
'N' as ORP_RCRD_FLAG,
IMHISTG.loaddts as ZONE3_LOD_DTE,
--IMHIST.ACREC AS CURR_RCRD_FLAG,
IMHISTG.REFNO AS PROD_MVMNT_DOC_NBR,
IMHISTG.TCODE AS MVMNT_TYP_LKEY,
 ITMPLNG.MINQ AS PROD_MAX_ORD_QTY,
 ITMPLNG.MAXQ AS PROD_MIN_ORD_QTY,
ITMPLNG.MULQ AS PROD_LOT_ORD_QTY,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTG.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTG.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTG.UPDDT), 7, 0), 6, 2)))) as PROD_MVMNT_DOC_DTE,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTG.TRNDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTG.TRNDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTG.TRNDT), 7, 0), 6, 2)))) as PROD_MVMNT_POST_DTE,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTG.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTG.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTG.UPDDT), 7, 0), 6, 2)))) as PROD_MVMNT_DOC_ENTR_DTE,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTG.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTG.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTG.UPDDT), 7, 0), 6, 2)))) as PROD_MVMNT_DOC_CHG_DTE,
IMHISTG.LGWNO AS GOODS_RCPT_NBR,
IMHISTG.ITNBR AS PROD_KEY,
CONCAT('MAR','~',IMHISTG.HOUSE) AS LOC_KEY,
IMHISTG.PLANN AS PROD_RESP_PRSN_NAME
FROM {{source('INFORXA','IMHISTMAR')}} IMHISTG
LEFT JOIN {{source('INFORXA','ITMPLN_MAR')}} ITMPLNG
ON ITMPLNG.ITNB = IMHISTG.ITNBR
AND ITMPLNG.WHID = IMHISTG.HOUSE
),
--###################################################################################
IMHISTRVI AS(
select upper(concat(coalesce(trim(IMHISTH.ITNBR)::varchar, ''),'~',
                    coalesce(trim(IMHISTH.HOUSE)::varchar, ''),'~',
                    coalesce(trim(IMHISTH.TAID)::varchar, ''),'~',
                    coalesce(trim(IMHISTH.TCODE)::varchar, ''),'~',
                    coalesce(trim(IMHISTH.PSTMAV)::varchar, ''),'~','IMHIST_RVI')) as INVTY_MVMNT_HDR_KEY,
'INFORXA' as SRC_SYS_KEY,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTH.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTH.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTH.UPDDT), 7, 0), 6, 2)))) as SRC_RCRD_CREATE_DTE,
null as SRC_RCRD_CREATE_USERID,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTH.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTH.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTH.UPDDT), 7, 0), 6, 2)))) as SRC_RCRD_UPD_DTE,
null as SRC_RCRD_UPD_USERID,
md5(upper(concat(coalesce(trim(IMHISTH.ITNBR)::varchar, ''),'~',
                    coalesce(trim(IMHISTH.HOUSE)::varchar, ''),'~',
                    coalesce(trim(IMHISTH.TAID)::varchar, ''),'~',
                    coalesce(trim(IMHISTH.TCODE)::varchar, ''),'~',
                    coalesce(trim(IMHISTH.PSTMAV)::varchar, ''),'~','IMHIST_RVI'))) as RCRD_HASH_KEY,
'N' as DEL_FROM_SRC_FLAG,
'EDW_INFORXA_INVENTORY_MOVEMENT_HEADER_VW' as ETL_INS_PID,
current_timestamp::timestamp_ntz as ETL_INS_DTE,
'EDW_INFORXA_INVENTORY_MOVEMENT_HEADER_VW' as ETL_UPD_PID,
current_timestamp::timestamp_ntz as ETL_UPD_DTE,
IMHISTH.loaddts as EFF_DTE,
'9999-12-31 00:00:00.000' as EXPR_DTE,
'Y' as CURR_RCRD_FLAG,
'N' as ORP_RCRD_FLAG,
IMHISTH.loaddts as ZONE3_LOD_DTE,
--IMHIST.ACREC AS CURR_RCRD_FLAG,
IMHISTH.REFNO AS PROD_MVMNT_DOC_NBR,
IMHISTH.TCODE AS MVMNT_TYP_LKEY,
 ITMPLNH.MINQ AS PROD_MAX_ORD_QTY,
 ITMPLNH.MAXQ AS PROD_MIN_ORD_QTY,
ITMPLNH.MULQ AS PROD_LOT_ORD_QTY,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTH.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTH.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTH.UPDDT), 7, 0), 6, 2)))) as PROD_MVMNT_DOC_DTE,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTH.TRNDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTH.TRNDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTH.TRNDT), 7, 0), 6, 2)))) as PROD_MVMNT_POST_DTE,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTH.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTH.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTH.UPDDT), 7, 0), 6, 2)))) as PROD_MVMNT_DOC_ENTR_DTE,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTH.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTH.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTH.UPDDT), 7, 0), 6, 2)))) as PROD_MVMNT_DOC_CHG_DTE,
IMHISTH.LGWNO AS GOODS_RCPT_NBR,
IMHISTH.ITNBR AS PROD_KEY,
CONCAT('RVI','~',IMHISTH.HOUSE) AS LOC_KEY,
IMHISTH.PLANN AS PROD_RESP_PRSN_NAME
FROM {{source('INFORXA','IMHISTRVI')}} IMHISTH
LEFT JOIN {{source('INFORXA','ITMPLN_RVI')}} ITMPLNH
ON ITMPLNH.ITNB = IMHISTH.ITNBR
AND ITMPLNH.WHID = IMHISTH.HOUSE
),
--###################################################################################
IMHISTSHB AS(
select upper(concat(coalesce(trim(IMHISTI.ITNBR)::varchar, ''),'~',
                    coalesce(trim(IMHISTI.HOUSE)::varchar, ''),'~',
                    coalesce(trim(IMHISTI.TAID)::varchar, ''),'~',
                    coalesce(trim(IMHISTI.TCODE)::varchar, ''),'~',
                    coalesce(trim(IMHISTI.PSTMAV)::varchar, ''),'~','IMHIST_SHB')) as INVTY_MVMNT_HDR_KEY,
'INFORXA' as SRC_SYS_KEY,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTI.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTI.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTI.UPDDT), 7, 0), 6, 2)))) as SRC_RCRD_CREATE_DTE,
null as SRC_RCRD_CREATE_USERID,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTI.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTI.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTI.UPDDT), 7, 0), 6, 2)))) as SRC_RCRD_UPD_DTE,
null as SRC_RCRD_UPD_USERID,
md5(upper(concat(coalesce(trim(IMHISTI.ITNBR)::varchar, ''),'~',
                    coalesce(trim(IMHISTI.HOUSE)::varchar, ''),'~',
                    coalesce(trim(IMHISTI.TAID)::varchar, ''),'~',
                    coalesce(trim(IMHISTI.TCODE)::varchar, ''),'~',
                    coalesce(trim(IMHISTI.PSTMAV)::varchar, ''),'~','IMHIST_SHB'))) as RCRD_HASH_KEY,
'N' as DEL_FROM_SRC_FLAG,
'EDW_INFORXA_INVENTORY_MOVEMENT_HEADER_VW' as ETL_INS_PID,
current_timestamp::timestamp_ntz as ETL_INS_DTE,
'EDW_INFORXA_INVENTORY_MOVEMENT_HEADER_VW' as ETL_UPD_PID,
current_timestamp::timestamp_ntz as ETL_UPD_DTE,
IMHISTI.loaddts as EFF_DTE,
'9999-12-31 00:00:00.000' as EXPR_DTE,
'Y' as CURR_RCRD_FLAG,
'N' as ORP_RCRD_FLAG,
IMHISTI.loaddts as ZONE3_LOD_DTE,
--IMHIST.ACREC AS CURR_RCRD_FLAG,
IMHISTI.REFNO AS PROD_MVMNT_DOC_NBR,
IMHISTI.TCODE AS MVMNT_TYP_LKEY,
 ITMPLNI.MINQ AS PROD_MAX_ORD_QTY,
 ITMPLNI.MAXQ AS PROD_MIN_ORD_QTY,
ITMPLNI.MULQ AS PROD_LOT_ORD_QTY,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTI.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTI.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTI.UPDDT), 7, 0), 6, 2)))) as PROD_MVMNT_DOC_DTE,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTI.TRNDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTI.TRNDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTI.TRNDT), 7, 0), 6, 2)))) as PROD_MVMNT_POST_DTE,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTI.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTI.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTI.UPDDT), 7, 0), 6, 2)))) as PROD_MVMNT_DOC_ENTR_DTE,
dateadd(year,1900,try_to_date(concat(left(lpad(trim(IMHISTI.UPDDT), 7, 0), 3),'-',substring(lpad(trim(IMHISTI.UPDDT), 7, 0), 4, 2),'-',substring(lpad(trim(IMHISTI.UPDDT), 7, 0), 6, 2)))) as PROD_MVMNT_DOC_CHG_DTE,
IMHISTI.LGWNO AS GOODS_RCPT_NBR,
IMHISTI.ITNBR AS PROD_KEY,
CONCAT('SHB','~',IMHISTI.HOUSE) AS LOC_KEY,
IMHISTI.PLANN AS PROD_RESP_PRSN_NAME
FROM {{source('INFORXA','IMHISTSHB')}} IMHISTI
LEFT JOIN {{source('INFORXA','ITMPLN_SHB')}} ITMPLNI
ON ITMPLNI.ITNB = IMHISTI.ITNBR
AND ITMPLNI.WHID = IMHISTI.HOUSE
),
--##########################################################################################
IMHIST AS(
SELECT * FROM IMHISTAUS
UNION
SELECT * FROM IMHISTCANS
UNION
SELECT * FROM IMHISTCANW
UNION
SELECT * FROM IMHISTCHI
UNION
SELECT * FROM IMHISTMAR
UNION
SELECT * FROM IMHISTRVI
UNION
SELECT * FROM IMHISTSHB
)
SELECT * FROM IMHIST

-- UNION
-- SELECT * FROM IMHISTCANS
-- UNION
-- SELECT * FROM IMHISTCANW
-- UNION
-- SELECT * FROM IMHISTCHI
-- UNION
-- SELECT * FROM IMHISTLEI
-- UNION
-- SELECT * FROM IMHISTMAR
-- UNION
-- SELECT * FROM IMHISTRVI
-- UNION
-- SELECT * FROM IMHISTSHB