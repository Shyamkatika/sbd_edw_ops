Select
UPPER(CONCAT(coalesce(BOM.ITEM::varchar,''),'~',coalesce(BOM.SUBORD::varchar,''),'~' ,coalesce(BOM.LOC::varchar,''),'~' ,coalesce(BOM.BOMNUM::varchar,'')
,'~' ,coalesce(BOM.EFF::varchar,''),'~' ,coalesce(BOM.OFFSET::varchar,''))) AS SUPPLY_PLNG_PROD_BOM_VW_KEY,
'JDAEDW' AS {{var('column_srcsyskey')}},
MD5(UPPER(CONCAT(coalesce(BOM.ITEM::varchar,''),'~',coalesce(BOM.SUBORD::varchar,''),'~' ,coalesce(BOM.LOC::varchar,''),'~' ,coalesce(BOM.BOMNUM::varchar,'')
,'~' ,coalesce(BOM.EFF::varchar,''),'~' ,coalesce(BOM.OFFSET::varchar,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
BOM.LOADDTS AS {{var('column_z3loddtm')}},
BOM.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
BOM.BOMNUM AS PROD_BOM_NBR,
BOM.CONSUMSTEPNUM AS PRODTN_STEP_NBR,
BOM.DISC AS BOM_CMPNT_DSCTU_DTE,
BOM.DRAWQTY AS PRNT_SKU_SUBORDINATE_QTY,
BOM.DRAWTYPE AS BOM_DRAWN_TYP,
BOM.ECN AS ENGNR_CHG_NTFCN,
BOM.EFF AS BOM_CMPNT_EFF_DTE,
BOM.ENABLEOPT AS ENABLE_OPT_NBR,
case when length(trim(UPPER(BOM.ITEM)))<1 then {{var('default_mapkey')}} when UPPER (BOM.ITEM) IS NULL then  {{var('default_mapkey')}} else UPPER (BOM.ITEM) end AS PRNT_BOM_PROD_KEY,
case when length(trim(UPPER(BOM.LOC)))<1 then {{var('default_mapkey')}} when UPPER (BOM.LOC) IS NULL then  {{var('default_mapkey')}} else UPPER (BOM.LOC) end AS PRNT_BOM_LOC_KEY,
BOM.MIXFACTOR AS PROD_VER_FACTOR,
BOM.OFFSET AS BOM_CMPNT_AVAILAIBLITY,
BOM.QTYPERASSEMBLY AS SKU_ASSEMBLY_QTY,
BOM.QTYUOM AS UOM_QTY,
BOM.SHRINKAGEFACTOR AS SHRINKAGE_FACTOR,
BOM.SPLITFACTOR AS SPLIT_FACTOR,
case when length(trim(UPPER(BOM.SUBORD)))<1 then {{var('default_mapkey')}} when UPPER (BOM.SUBORD) IS NULL then  {{var('default_mapkey')}} else UPPER (BOM.SUBORD) end AS CMPNT_BOM_PROD_KEY,
case when length(trim(UPPER(BOM.SUBORDLOC)))<1 then {{var('default_mapkey')}} when UPPER (BOM.SUBORDLOC) IS NULL then  {{var('default_mapkey')}} else UPPER (BOM.SUBORDLOC) end AS CMPNT_BOM_LOC_KEY,
CASE WHEN BOM.SUPERSEDESW = '1' THEN 'Y' ELSE 'N' END AS CMPNT_BOM_SUPERSEDE_FLAG,
BOM.UNITCONVFACTOR AS UNIT_CONV_FACTOR,
BOM.YIELDFACTOR AS YIELD_FACTOR
FROM {{source('JDAEDW','BOM')}} BOM
