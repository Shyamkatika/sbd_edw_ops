select 
UPPER(CONCAT(COALESCE(trim(UPISU.user_field_1)::VARCHAR,''),'~',COALESCE(trim(UPISU.PART_ID)::VARCHAR,''),'~',COALESCE(trim(UPISU.Identity_Column)::VARCHAR,''))) AS INVTY_CYCLE_CNT_KEY,
MD5(UPPER(CONCAT(COALESCE(trim(UPISU.user_field_1)::VARCHAR,''),'~',COALESCE(trim(UPISU.PART_ID)::VARCHAR,''),'~',COALESCE(trim(UPISU.Identity_Column)::VARCHAR,'')))) AS RCRD_HASH_KEY,
'EXCEL_ERRORPROOF' AS SRC_SYS_KEY,
date(UPISU.created_datetime) AS SRC_RCRD_CREATE_DTE,
UPISU.Created_By_UserID AS SRC_RCRD_CREATE_USERID,
date(UPISU.update_datetime ) AS SRC_RCRD_UPD_DTE,
UPISU.LOADDTS AS ZONE3_LOD_DTE,
UPISU.SITE_ID AS LOC_KEY,
UPISU.Part_ID AS PROD_KEY,
UPISU.Location_ID AS SLOC_ID,
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
{{var('default_n')}} AS  {{var('column_DEL_FROM_SRC_FLAG')}}, 
UPISU.Unit_Measure AS BASE_UNIT_QTY,
UPISU.Created_By_UserID AS ASGN_USER_ID,
'OUTDOOR' AS BUS_UNIT_KEY,
IN_MSTR.in_abc AS CYCLE_CNT_CLS_KEY,
UPISU.Sub_Account_Code AS GL_CLS_CD_LKEY,
(CASE WHEN UPISU.batch_flag=1 THEN date(UPISU.created_datetime)
 ELSE NULL END) AS CYCLE_CNT_BATCH_DTE,
date(UPISU.created_datetime) AS CNT_CMPLT_DTE,
date(UPISU.created_datetime) AS CYCLE_CNT_ACT_DTE,
date(UPISU.created_datetime) AS CYCLE_CNT_SYS_UPD_DTE,
date(UPISU.created_datetime) AS CYCLE_CNT_SYS_ENTR_DTE,
UPISU.Batchnum AS CYCLE_CNT_BATCH_NBR,
UPISU.Identity_Column AS CYCLE_CNT_NBR,
UPISU.Identity_Column AS CYCLE_CNT_ASGN_NBR,
UPISU.Part_ID AS USER_DEFINED_PROD_NBR,
UPISU.Lot_Serial_ID AS LOT_SERL_NBR,
PL_MSTR.pl_prod_line AS PROD_FAMILY_SLS_NBR,
PL_MSTR.pl_prod_line AS FAMILY_SECT_KEY,
LD_DET.ld_loc AS INVTY_BIN_NBR,
UPISU.Project_ID AS PGM_NBR,
case when location_id like '9%' or location_id like '7%' then location_id else '0' end AS WORK_STATION_NBR,
CASE WHEN UPISU.User_Field_2 = '' THEN 0 ELSE UPISU.User_Field_2 END AS  BASE_CNT,
UPISU.User_Field_3 AS COUNTED_PROD_QTY,
CASE WHEN UPISU.User_Field_4 = '' THEN '0' 
ELSE UPISU.User_Field_4 END AS CATCH_QTY,
CASE WHEN UPISU.User_Field_2 = '' THEN 0 ELSE UPISU.User_Field_2 END AS CYCLE_CNT_START_ON_HAND_QTY,
--CAST(CONCAT(UPISU.user_field_2,'~',IN_MSTR.in_cur_set) AS NUMBER(28,10)) AS CYCLE_CNT_START_ON_HAND_AMT,
--UPISU.user_field_2 AS CYCLE_CNT_START_ON_HAND_AMT, 
CONCAT(UPISU.user_field_3) AS COUNTED_PROD_AMT,
 --,'~',IN_MSTR.in_cur_set) 
(CASE WHEN UPPER(TRIM(SCT_DET.SCT_SIM))='CURRENT' THEN SCT_DET.sct_cst_tot 
ELSE NULL END) AS UNIT_PROD_COST,
CASE WHEN UPISU.User_Field_2 = '' THEN 0 ELSE UPISU.User_Field_2 END AS CYCLE_CNT_EXPCT_START_ON_HAND_QTY,
IN_MSTR.in_cnt_date AS CYCLE_CNT_DTE,
CASE WHEN UPISU.user_field_2 IS NULL OR UPISU.user_field_2 = '' THEN 'N' ELSE 'Y' END AS DISCREPANCY_FLAG,
IN_MSTR.in_wh AS WMS_SCHEMA_NAME,
UPISU.Location_ID AS PUT_ZONE_ID,
IN_MSTR.in_abc AS CYCLE_CNT_CLS_LKEY
,(CASE WHEN IN_MSTR.in_cur_set = '' THEN 0 ELSE IN_MSTR.in_cur_set END) AS PROD_UNIT_COST 
-- IN_MSTR.in_cur_set AS PROD_UNIT_COST,
,UPISU.Part_ID AS PROD_ID
FROM  {{source("EXCEL_ERRORPROOF","UPISU")}} UPISU 
LEFT JOIN {{source("EXCEL_QADDB","IN_MSTR")}} IN_MSTR 
ON UPPER(TRIM(IN_MSTR.IN_PART))= UPPER(TRIM(UPISU.PART_ID))
AND UPPER(TRIM(UPISU.SITE_ID))= UPPER(TRIM(IN_MSTR.IN_SITE))
AND UPPER(TRIM(IN_MSTR.IN_DOMAIN)) ='EXCEL'
-- WHERE quantity <>0 and remarks = 'INV COUNT'                 
LEFT JOIN {{source("EXCEL_QADDB","LOC_MSTR")}} LOC_MSTR
ON LOC_MSTR.LOC_SITE =UPISU.SITE_ID 
AND LOC_MSTR.LOC_LOC = UPISU.LOCATION_ID 
AND UPPER(TRIM(LOC_MSTR.LOC_DOMAIN)) = 'EXCEL'
-- WHERE quantity <>0 and remarks = 'INV COUNT'  desc table "TEST_RAW"."EXCEL_ERRORPROOF"."UPISU"
LEFT JOIN {{source("EXCEL_QADDB","PT_MSTR")}} PT_MSTR
ON UPPER(TRIM(PT_MSTR.PT_PART)) = UPPER(TRIM(UPISU.PART_ID))
AND UPPER(TRIM(PT_MSTR.PT_DOMAIN))='EXCEL'
-- WHERE quantity <>0 and remarks = 'INV COUNT'
LEFT JOIN {{source("EXCEL_QADDB","PL_MSTR")}}  PL_MSTR            
ON UPPER(TRIM(PL_MSTR.PL_PROD_LINE)) = UPPER(TRIM(PT_MSTR.PT_PROD_LINE))  --
AND UPPER(TRIM(PL_MSTR.PL_DOMAIN))='EXCEL'
-- WHERE quantity <>0 and remarks = 'INV COUNT' --5755' DESC TABLE "TEST_RAW"."EXCEL_QADDB"."LD_DET" LD_LOC,LD_PART,LD_LOT,LD_REF,LD_SITE,LD_DOMAIN
left join {{source("EXCEL_QADDB","LD_DET")}} LD_DET
ON UPPER(TRIM(LD_DET.LD_SITE)) =UPPER(TRIM(UPISU.SITE_ID)) 
AND UPPER(TRIM(LD_DET.LD_PART))=UPPER(TRIM(UPISU.PART_ID))
AND UPPER(TRIM(LD_DET.LD_LOT))= UPPER(TRIM(UPISU.LOT_SERIAL_ID))
AND UPPER(TRIM(LD_DET.LD_LOC))=UPPER(TRIM(UPISU.LOCATION_ID))
AND UPPER(TRIM(LD_DET.LD_DOMAIN)) ='EXCEL'
--WHERE quantity <>0 and remarks = 'INV COUNT'
--LEFT JOIN "TEST_RAW"."EXCEL_QADDB"."VW_TR_HIST" TR_HIST
--QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(tr_part),UPPER(tr_domain) ORDER BY TR_TRNBR ) =1 )
--ON UPPER(TR_HIST.TR_DATE) = DATE(UPISU.created_datetime) 
--AND UPPER(TRIM(TR_HIST.TR_PART))= UPPER(TRIM(UPISU.PART_ID))
--AND UPPER(TRIM(TR_HIST.TR_RMKS)) = UPPER(TRIM(UPISU.REMARKS))
--AND UPPER(TRIM(TR_HIST.TR_LOC)) = UPPER(TRIM(UPISU.LOCATION_ID))
--AND UPPER(TRIM(TR_HIST.TR_SITE))= UPPER(TRIM(UPISU.SITE_ID))
--AND UPPER(TRIM(TR_HIST.TR_DOMAIN))='EXCEL'
--AND UPPER(TRIM(TR_HIST.TR_REF))=UPPER(TRIM(UPISU.REFERENCE_ID))
--AND UPPER(TRIM(TR_HIST.TR_TYPE)) IN ('RJCT-WO','RCT_WO')
--WHERE quantity <>0 and remarks = 'INV COUNT'
LEFT JOIN {{source("EXCEL_QADDB","SCT_DET")}} SCT_DET
on UPPER(SCT_DET.sct_domain) = 'EXCEL' 
and UPPER(TRIM(SCT_DET.sct_site)) = UPPER(TRIM(IN_MSTR.in_site)) 
and UPPER(TRIM(SCT_DET.sct_part)) = UPPER(TRIM(IN_MSTR.in_part)) 
and UPPER(TRIM(SCT_DET.sct_sim)) = UPPER(TRIM(IN_MSTR.in_gl_set))
-- WHERE quantity <>0 and remarks = 'INV COUNT' DESC TABLE "TEST_RAW"."EXCEL_QADDB"."PT_MSTR"  TR_TRNBR
-- LEFT JOIN "TEST_RAW"."EXCEL_QADDB"."VW_WO_MSTR" WO_MSTR
-- ON  UPPER(TRIM(TR_HIST.TR_NBR)) = UPPER(TRIM(WO_MSTR.WO_NBR))
-- AND UPPER(TRIM(TR_HIST.TR_PART))= UPPER(TRIM(WO_MSTR.WO_PART))
-- AND UPPER(TRIM(TR_HIST.TR_LOT)) = UPPER(TRIM(WO_MSTR.WO_LOT))
-- AND UPPER(TRIM(TR_HIST.TR_TYPE)) IN ('RJCT-WO','RCT_WO')
WHERE quantity <>0 and UPPER(TRIM(remarks)) = 'INV COUNT'
-- =======
--             'EXCEL_ERRORPROOF' AS SRC_SYS_KEY,
--              date(UPISU.created_datetime) AS SRC_RCRD_CREATE_DTE,
--              UPISU.Created_By_UserID AS SRC_RCRD_CREATE_USERID,
--                  date(UPISU.update_datetime ) AS SRC_RCRD_UPD_DTE,
--                  UPISU.LOADDTS AS ZONE3_LOD_DTE,
--                  UPISU.SITE_ID AS LOC_KEY,
--                  UPISU.Part_ID AS PROD_KEY,
--                  UPISU.Location_ID AS SLOC_ID
--                  ,'Y' AS  CURR_RCRD_FLAG
--                 ,'N' AS  ORP_RCRD_FLAG
--                  ,UPISU.Unit_Measure AS BASE_UNIT_QTY,
--                  UPISU.Created_By_UserID AS ASGN_USER_ID,
--                  'OUTDOOR' AS BUS_UNIT_KEY,
--                  IN_MSTR.in_abc AS CYCLE_CNT_CLS_KEY,
--                  UPISU.Sub_Account_Code AS GL_CLS_CD_LKEY,
--                  (CASE WHEN UPISU.batch_flag=1 THEN date(UPISU.created_datetime)
--                   ELSE NULL END) AS CYCLE_CNT_BATCH_DTE,
--                  date(UPISU.created_datetime) AS CNT_CMPLT_DTE,
--                  date(UPISU.created_datetime) AS CYCLE_CNT_ACT_DTE,
--                  date(UPISU.created_datetime) AS CYCLE_CNT_SYS_UPD_DTE,
--                  date(UPISU.created_datetime) AS CYCLE_CNT_SYS_ENTR_DTE,
--                  UPISU.Batchnum AS CYCLE_CNT_BATCH_NBR,
--                  UPISU.Identity_Column AS CYCLE_CNT_NBR,
--                  UPISU.Identity_Column AS CYCLE_CNT_ASGN_NBR,
--                  UPISU.Part_ID AS USER_DEFINED_PROD_NBR,
--                  UPISU.Lot_Serial_ID AS LOT_SERL_NBR,
--                  PL_MSTR.pl_prod_line AS PROD_FAMILY_SLS_NBR,
--                  PL_MSTR.pl_prod_line AS FAMILY_SECT_KEY,
--                  LD_DET.ld_loc AS INVTY_BIN_NBR,
--                  UPISU.Project_ID AS PGM_NBR,
--                  WO_MSTR.wo_line AS WORK_STATION_NBR,
--                  TR_HIST.tr_qty_chg AS BASE_CNT,
--                  UPISU.User_Field_3 AS COUNTED_PROD_QTY,
--                  UPISU.User_Field_4 AS CATCH_QTY,
--                  UPISU.User_Field_2 AS CYCLE_CNT_START_ON_HAND_QTY,
--                 --  CAST(CONCAT(UPISU.user_field_2,'~',IN_MSTR.in_cur_set) AS NUMBER(28,10)) AS CYCLE_CNT_START_ON_HAND_AMT,
--                 -- UPISU.user_field_2 AS CYCLE_CNT_START_ON_HAND_AMT, 
--                  CONCAT(UPISU.user_field_3) AS COUNTED_PROD_AMT,
--                  --,'~',IN_MSTR.in_cur_set) 
--                  (CASE WHEN UPPER(TRIM(SCT_DET.SCT_SIM))='CURRENT' THEN SCT_DET.sct_cst_tot 
--                   ELSE NULL END) AS UNIT_PROD_COST,
--                 --  UPISU.User_Field_2 AS CYCLE_CNT_EXPCT_START_ON_HAND_QTY,
--                  IN_MSTR.in_cnt_date AS CYCLE_CNT_DTE,
--                  (CASE WHEN UPISU.user_field_2 <> 0 THEN 'Y'
--                  ELSE 'N' END) AS DISCREPANCY_FLAG,
--                  IN_MSTR.in_wh AS WMS_SCHEMA_NAME,
--                  UPISU.Location_ID AS PUT_ZONE_ID,
--                  IN_MSTR.in_abc AS CYCLE_CNT_CLS_LKEY
--                  ,(CASE
--         WHEN IN_MSTR.in_cur_set = '' THEN 0
--         ELSE IN_MSTR.in_cur_set
--     END) AS PROD_UNIT_COST 
--                 --  IN_MSTR.in_cur_set AS PROD_UNIT_COST,
--                  ,UPISU.Part_ID AS PROD_ID
-- FROM  {{source("EXCEL_ERRORPROOF","UPISU")}} AS UPISU
-- LEFT JOIN
-- {{source("EXCEL_QADDB","IN_MSTR")}} IN_MSTR -- "TEST_RAW"."EXCEL_QADDB"."IN_MSTR" IN_MSTR 
--               ON UPPER(TRIM(IN_MSTR.IN_PART))= UPPER(TRIM(UPISU.PART_ID))
--               AND UPPER(TRIM(UPISU.SITE_ID))= UPPER(TRIM(IN_MSTR.IN_SITE))
--                AND UPPER(TRIM(IN_MSTR.IN_DOMAIN)) ='EXCEL'
-- --                   WHERE quantity <>0 and remarks = 'INV COUNT'
                 
-- LEFT JOIN {{source("EXCEL_QADDB","LOC_MSTR")}} LOC_MSTR--"TEST_RAW"."EXCEL_QADDB"."VW_LOC_MSTR" LOC_MSTR
-- ON LOC_MSTR.LOC_SITE =UPISU.SITE_ID 
-- AND LOC_MSTR.LOC_LOC = UPISU.LOCATION_ID 
-- AND UPPER(TRIM(LOC_MSTR.LOC_DOMAIN)) = 'EXCEL'
-- --                 WHERE quantity <>0 and remarks = 'INV COUNT'  desc table "TEST_RAW"."EXCEL_ERRORPROOF"."UPISU"
-- LEFT JOIN {{source("EXCEL_QADDB","PT_MSTR")}} PT_MSTR --"TEST_RAW"."EXCEL_QADDB"."VW_PT_MSTR" PT_MSTR
--                 ON UPPER(TRIM(PT_MSTR.PT_PART)) = UPPER(TRIM(UPISU.PART_ID))
--                 AND UPPER(TRIM(PT_MSTR.PT_DOMAIN))='EXCEL'
-- --                WHERE quantity <>0 and remarks = 'INV COUNT'
-- LEFT JOIN {{source("EXCEL_QADDB","PL_MSTR")}} PL_MSTR            
--  ON UPPER(TRIM(PL_MSTR.PL_PROD_LINE)) = UPPER(TRIM(PT_MSTR.PT_PROD_LINE)) 
--  AND UPPER(TRIM(PL_MSTR.PL_DOMAIN))='EXCEL'
-- --  WHERE quantity <>0 and remarks = 'INV COUNT' --5755' DESC TABLE "TEST_RAW"."EXCEL_QADDB"."LD_DET" LD_LOC,LD_PART,LD_LOT,LD_REF,LD_SITE,LD_DOMAIN
-- left join {{source("EXCEL_QADDB","LD_DET")}} LD_DET
--                   ON UPPER(TRIM(LD_DET.LD_SITE)) =UPPER(TRIM(UPISU.SITE_ID)) 
--                AND UPPER(TRIM(LD_DET.LD_PART))=UPPER(TRIM(UPISU.PART_ID))
--                AND UPPER(TRIM(LD_DET.LD_LOT))= UPPER(TRIM(UPISU.LOT_SERIAL_ID))
--                AND UPPER(TRIM(LD_DET.LD_LOC))=UPPER(TRIM(UPISU.LOCATION_ID))
--                AND UPPER(TRIM(LD_DET.LD_DOMAIN)) ='EXCEL'
-- --                 WHERE quantity <>0 and remarks = 'INV COUNT'
--                    LEFT JOIN {{source("EXCEL_QADDB","TR_HIST")}} TR_HIST
-- --                   QUALIFY ROW_NUMBER() OVER (PARTITION BY UPPER(tr_part),UPPER(tr_domain) ORDER BY TR_TRNBR ) =1 )
--                    ON UPPER(TR_HIST.TR_DATE) = DATE(UPISU.created_datetime) 
--              AND UPPER(TRIM(TR_HIST.TR_PART))= UPPER(TRIM(UPISU.PART_ID))
--              AND UPPER(TRIM(TR_HIST.TR_RMKS)) = UPPER(TRIM(UPISU.REMARKS))
--              AND UPPER(TRIM(TR_HIST.TR_LOC)) = UPPER(TRIM(UPISU.LOCATION_ID))
--              AND UPPER(TRIM(TR_HIST.TR_SITE))= UPPER(TRIM(UPISU.SITE_ID))
--             AND UPPER(TRIM(TR_HIST.TR_DOMAIN))='EXCEL'
--             AND UPPER(TRIM(TR_HIST.TR_REF))=UPPER(TRIM(UPISU.REFERENCE_ID))
--             AND UPPER(TRIM(TR_HIST.TR_TYPE)) IN ('RJCT-WO','RCT_WO')
-- --                 WHERE quantity <>0 and remarks = 'INV COUNT'
--                   LEFT JOIN {{source("EXCEL_QADDB","SCT_DET")}} SCT_DET
--                  on UPPER(sct_domain) = 'EXCEL' 
--                  and UPPER(TRIM(SCT_DET.sct_site)) = UPPER(TRIM(IN_MSTR.in_site)) 
--                  and UPPER(TRIM(SCT_DET.sct_part)) = UPPER(TRIM(IN_MSTR.in_part)) 
--                  and UPPER(TRIM(SCT_DET.sct_sim)) = UPPER(TRIM(IN_MSTR.in_gl_set))
-- --                           WHERE quantity <>0 and remarks = 'INV COUNT' DESC TABLE "TEST_RAW"."EXCEL_QADDB"."PT_MSTR"  TR_TRNBR
-- LEFT JOIN {{source("EXCEL_QADDB","WO_MSTR")}} WO_MSTR
-- ON  UPPER(TRIM(TR_HIST.TR_NBR)) = UPPER(TRIM(WO_MSTR.WO_NBR))
-- AND UPPER(TRIM(TR_HIST.TR_PART))= UPPER(TRIM(WO_MSTR.WO_PART))
-- AND UPPER(TRIM(TR_HIST.TR_LOT)) = UPPER(TRIM(WO_MSTR.WO_LOT))
-- AND UPPER(TRIM(TR_HIST.TR_TYPE)) IN ('RJCT-WO','RCT_WO')
--                  WHERE quantity <>0 and UPPER(TRIM(remarks)) = 'INV COUNT'
