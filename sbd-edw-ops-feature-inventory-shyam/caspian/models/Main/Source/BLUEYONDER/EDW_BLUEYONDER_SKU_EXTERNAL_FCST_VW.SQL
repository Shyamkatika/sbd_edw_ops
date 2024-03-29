SELECT
UPPER(CONCAT(COALESCE(SKUEXTERNALFCST.ITEM::VARCHAR,''),'~',COALESCE(SKUEXTERNALFCST.LOC::VARCHAR,''),'~',COALESCE(SKUEXTERNALFCST.PROJECT::VARCHAR,''),'~',COALESCE(SKUEXTERNALFCST.STARTDATE::VARCHAR,''))) AS SKU_EXTERNAL_FCST_KEY,
'BLUEYONDER' AS  {{var('column_srcsyskey')}},  
MD5(UPPER(CONCAT(COALESCE(SKUEXTERNALFCST.ITEM::VARCHAR,''),'~',COALESCE(SKUEXTERNALFCST.LOC::VARCHAR,''),'~',COALESCE(SKUEXTERNALFCST.PROJECT::VARCHAR,''),'~',COALESCE(SKUEXTERNALFCST.STARTDATE::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
SKUEXTERNALFCST.LOADDTS AS  {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
SKUEXTERNALFCST.LOADDTS AS {{var('column_z3loddtm')}},
SKUEXTERNALFCST.ITEM AS PROD_KEY,
SKUEXTERNALFCST.LOC AS LOC_KEY,
SKUEXTERNALFCST.ITEM AS MATL_NBR,
SKUEXTERNALFCST.LOC AS LOC_ID,
SKUEXTERNALFCST.DUR AS FCST_DUR,
SKUEXTERNALFCST.QTY AS FCST_QTY,
SKUEXTERNALFCST.PROJECT AS PROJ_NBR,
SKUEXTERNALFCST.STARTDATE AS FCST_START_DTE
FROM {{source('BLUEYONDER','SKUEXTERNALFCST')}} SKUEXTERNALFCST