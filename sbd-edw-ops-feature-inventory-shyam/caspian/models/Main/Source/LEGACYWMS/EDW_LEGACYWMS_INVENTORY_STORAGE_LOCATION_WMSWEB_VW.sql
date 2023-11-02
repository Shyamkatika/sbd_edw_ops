select
UPPER(CONCAT(coalesce(LOCATOR.LODNUM ::VARCHAR,''),'~','WMSWEB')) AS INVTY_SLOC_KEY,
'LEGACYWMS' AS {{var('column_srcsyskey')}},
MD5(UPPER(CONCAT(coalesce(LOCATOR.LODNUM::VARCHAR,''),'~','WMSWEB'))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
LOCATOR.LOADDTS AS {{var('column_z3loddtm')}},
LOCATOR.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
'WMSWEB' as WMS_SCHEMA_NAME,

LOCATOR.LODNUM AS PAL_NBR,
LOCATOR.CTRL_DT AS RCRD_PROC_DTE,
LOCATOR.CTRL_USER_ID AS USER_NAME,
LOCATOR.ORDER_NUMBER AS PROD_ORD_NBR,
LOCATOR.PACKAGE_ID AS PKG_NBR,
LOCATOR.TRACKING_NUMBER AS PKG_TRK_NBR,
LOCATOR.SCAC AS CARRIER_CD_NBR,
LOCATOR.TRAILER_ID AS TRAILER_NBR,

LOCATOR.LOCATION AS SLOC_ID,

--LOCATOR.LANE_NUMBER AS LANE_NBR,
LOCATOR.LANE_NUMBER AS LANE_NBR,
LOCATOR.DEST_SUPPLIER_ID AS SUPLR_NBR

FROM {{source('WMSWEB','LOCATOR')}} LOCATOR
