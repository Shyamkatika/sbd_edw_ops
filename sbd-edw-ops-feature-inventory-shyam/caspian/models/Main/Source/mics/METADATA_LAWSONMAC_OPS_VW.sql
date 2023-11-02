SELECT DISTINCT * FROM
(
SELECT 'WMWMSMP'AS SRCTBLNAME, 
'LAWSONMAC' AS SYSTEMNAME, 
'EDW_INVENTORY_STORAGE_LOCATION' AS TBLNAME, 
'INVTY_SLOC_KEY' AS DEST_CHANGE_KEY, 
'WMWMSMP_CHANGES' AS SOURCE_CHANGE_TBL, 
'WMDIV~WMWHSE~WMNAME' AS SOURCE_CHANGE_KEY, 
'SCD2' AS SCDTYPE ,
NULL AS SRCTBLCONDITION

UNION ALL

SELECT 'VW_WMSLCMP'AS SRCTBLNAME, 
'LAWSONMAC' AS SYSTEMNAME, 
'EDW_INVENTORY_ON_HAND_SNAPSHOT' AS TBLNAME, 
'INVTY_ON_HAND_SNAPSHOT_KEY' AS DEST_CHANGE_KEY, 
'VW_WMSLCMP_CHANGES' AS SOURCE_CHANGE_TBL, 
'SLDIV~SLWHSE~SLBLOC~SLITEM~SLLTYP~SLDADD~SLSRRD' AS SOURCE_CHANGE_KEY, 
'SCD2' AS SCDTYPE ,
NULL AS SRCTBLCONDITION

UNION ALL

SELECT 'VW_WMTRNXP'AS SRCTBLNAME, 
'LAWSONMAC' AS SYSTEMNAME, 
'EDW_WAREHOUSE_TRANSFER_ORDER_LINE' AS TBLNAME, 
'WHSE_TRNSFR_ORD_LN_KEY' AS DEST_CHANGE_KEY, 
'VW_WMTRNXP_CHANGES' AS SOURCE_CHANGE_TBL, 
'TRDIV~TRTCDE~TRITEM~TRTSEQ~TRFWHS~TRRORD~TRDTRF~TRTIME~TRWSID~TRUSER' AS SOURCE_CHANGE_KEY, 
'SCD2' AS SCDTYPE ,
NULL AS SRCTBLCONDITION
)