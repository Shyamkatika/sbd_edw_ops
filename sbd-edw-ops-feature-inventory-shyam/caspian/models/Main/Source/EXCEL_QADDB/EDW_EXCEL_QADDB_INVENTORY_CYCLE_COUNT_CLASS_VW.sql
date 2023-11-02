select 
UPPER(CONCAT(COALESCE(trim(UPISU.user_field_1)::VARCHAR,''),'~',COALESCE(trim(UPISU.PART_ID)::VARCHAR,''),'~',COALESCE(trim(UPISU.Identity_Column)::VARCHAR,''))) AS CYCLE_CNT_CLS_KEY,
'EXCEL_ERRORPROOF' AS SRC_SYS_KEY,
NULL AS {{var('column_SRC_RCRD_CREATE_DTE')}},
upisu.Created_By_UserID AS {{var('column_SRC_RCRD_CREATE_USERID')}},
NULL AS {{var('column_SRC_RCRD_UPD_DTE')}},
NULL AS {{var('column_SRC_RCRD_UPD_USERID')}}, 
md5(UPPER(CONCAT(COALESCE(trim(UPISU.user_field_1)::VARCHAR,''),'~',COALESCE(trim(UPISU.PART_ID)::VARCHAR,''),'~',COALESCE(trim(UPISU.Identity_Column)::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS  {{var('column_DEL_FROM_SRC_FLAG')}},  
'{{model.name}}' AS  {{var('column_ETL_INS_PID')}},                                                                  
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS  {{var('column_ETL_UPD_PID')}},                                                                  
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_UPD_DTE')}},
upisu.Effective_Date AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}}, 
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
upisu.LOADDTS AS {{var('column_z3loddtm')}},
upisu.Created_By_UserID AS OPER_CD_TYP_LKEY,
upisu.User_Field_1 AS NEXT_CNT_TYP_LKEY,
upisu.User_Field_1 AS INVTY_ADJ_MTHD_TYP_LKEY,
upisu.Created_By_UserID AS CYCLE_CNT_CREATE_USER_KEY,
upisu.Part_ID AS CYCLE_CNT_ITEM_NBR,
CONCAT(upisu.part_id,upisu.location_id) AS CYCLE_CNT_LOC_CNT,
upisu.identity_column AS CYCLE_CNT_NBR,
wo_mstr.wo_line AS WORK_STATION_NBR,
upisu.Location_ID AS DEST_INVTY_LOC_KEY,
date(upisu.created_datetime) AS INVTY_STK_CALC_DTE,
upisu.SITE_ID AS STORE_LOC_KEY,
--upisu.distinct count (location_id) AS SLOC_CNT, work on this column
upisu.User_Field_1 AS CYCLE_CNT_TYP_LKEY,
CASE WHEN loc_mstr.loc_perm = 'TRUE' THEN 'Y' ELSE 'N' END AS LOC_CLOSE_TYP,
upisu.Remarks AS SUMM_ADJ_RESN_CD_DTE,
in_mstr.in_wh AS WMS_SCHEMA_NAME
FROM {{source("EXCEL_ERRORPROOF","UPISU")}} AS UPISU
LEFT JOIN {{source("EXCEL_QADDB","IN_MSTR")}} IN_MSTR
ON  UPPER(TRIM(IN_MSTR.IN_PART)) = UPPER(TRIM(UPISU.PART_ID)) 
AND UPPER(TRIM(IN_MSTR.IN_SITE)) = UPPER(TRIM(UPISU.SITE_ID))
AND UPPER(TRIM(IN_DOMAIN)) ='EXCEL' AND UPISU.quantity <>0 AND UPPER(TRIM(UPISU.remarks)) = 'INV COUNT'
LEFT JOIN {{source("EXCEL_QADDB","LOC_MSTR")}}  LOC_MSTR
ON  UPPER(TRIM(LOC_MSTR.LOC_LOC)) = UPPER(TRIM(UPISU.LOCATION_ID)) 
AND UPPER(TRIM(LOC_MSTR.LOC_SITE)) = UPPER(TRIM(UPISU.SITE_ID))
AND UPISU.quantity <> 0 AND UPPER(TRIM(UPISU.remarks)) = 'INV COUNT'
--WHERE UPPER(TRIM(IN_DOMAIN))= 'EXCEL'
LEFT JOIN {{source("EXCEL_QADDB","TR_HIST")}} AS TR_HIST
ON DATE(TR_HIST.TR_DATE) =  DATE(UPISU.CREATED_DATETIME)
AND UPPER(TRIM(TR_HIST.TR_SITE)) = UPPER(TRIM(UPISU.PART_ID))
AND UPPER(TRIM(TR_HIST.TR_RMKS)) = UPPER(TRIM(UPISU.REMARKS))
AND UPPER(TRIM(TR_HIST.TR_LOC))  = UPPER(TRIM(UPISU.LOCATION_ID))
AND UPPER(TR_DOMAIN)='EXCEL'
LEFT JOIN {{source("EXCEL_QADDB","WO_MSTR")}} WO_MSTR
ON  UPPER(TRIM(TR_HIST.TR_NBR)) = UPPER(TRIM(WO_MSTR.WO_NBR))
AND UPPER(TRIM(TR_HIST.TR_PART))= UPPER(TRIM(WO_MSTR.WO_PART))
AND UPPER(TRIM(TR_HIST.TR_LOT)) = UPPER(TRIM(WO_MSTR.WO_LOT))
AND TR_TYPE IN ('RJCT-WO','RCT-WO')
WHERE UPPER(TRIM(IN_DOMAIN))= 'EXCEL'
and UPISU.quantity <>0 AND UPPER(TRIM(UPISU.remarks)) = 'INV COUNT'