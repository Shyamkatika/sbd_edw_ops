
SELECT DISTINCT * FROM
(
SELECT 'VW_UPISU'AS SRCTBLNAME, 'EXCEL_ERRORPROOF' AS SYSTEMNAME, 'EDW_INVENTORY_CYCLE_COUNT' AS TBLNAME, 'INVTY_CYCLE_CNT_KEY' AS DEST_CHANGE_KEY, 'VW_UPISU_CHANGES' AS SOURCE_CHANGE_TBL, 'user_field_1~PART_ID~Identity_Column' AS SOURCE_CHANGE_KEY, 'SCD2' AS SCDTYPE ,NULL AS SRCTBLCONDITION 
UNION all
SELECT 'VW_UPISU' AS SRCTBLNAME, 'EXCEL_ERRORPROOF' AS SYSTEMNAME, 'EDW_INVENTORY_CYCLE_COUNT_CLASS' AS TBLNAME, 'CYCLE_CNT_CLS_KEY' AS DEST_CHANGE_KEY, 'VW_UPISU_CHANGES' AS SOURCE_CHANGE_TBL, 'USER_FIELD_1~PART_ID~IDENTITY_COLUMN' AS SOURCE_CHANGE_KEY, 'SCD2' AS SCDTYPE,NULL AS SRCTBLCONDITION
UNION ALL
SELECT 'VW_PHYSCNT'AS SRCTBLNAME, 'EXCEL_ERRORPROOF' AS SYSTEMNAME, 'EDW_INVENTORY_PHYSICAL_COUNT' AS TBLNAME, 'PHY_INVTY_KEY' AS DEST_CHANGE_KEY, 'VW_PHYSCNT_CHANGES' AS SOURCE_CHANGE_TBL, 'COUNT_TYPE~SITE_ID~LOCATION_ID~PART_ID~CREATED_DATETIME~COUNTER_ID' AS SOURCE_CHANGE_KEY, 'SCD2' AS SCDTYPE ,NULL AS SRCTBLCONDITION
)