select
UPPER(CONCAT(coalesce(IPH.PHWHSE::VARCHAR,''),'~',coalesce(IPH.PHTAG::VARCHAR,''))) AS PHY_INVTY_KEY,
'INFORLX' AS {{var('column_srcsyskey')}},
MD5(UPPER(CONCAT(coalesce(IPH.PHWHSE::VARCHAR,''),'~',coalesce(IPH.PHTAG::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
IPH.LOADDTS AS {{var('column_z3loddtm')}},
IPH.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},


case when length(trim(UPPER((IPH.PHWHSE)))) <1 then {{var('default_mapkey')}} when UPPER(IPH.PHWHSE)
IS NULL then {{var('default_mapkey')}} else UPPER(IPH.PHWHSE) end AS LOC_KEY,

case when length(trim(UPPER((IPH.PHPROD)))) <1 then {{var('default_mapkey')}} when UPPER(IPH.PHPROD)
IS NULL then {{var('default_mapkey')}} else UPPER(IPH.PHPROD) end AS PROD_KEY,

case when length(trim(UPPER((IPH.PHLOC)))) <1 then {{var('default_mapkey')}} when UPPER(IPH.PHLOC)
IS NULL then {{var('default_mapkey')}} else UPPER(IPH.PHLOC) end AS STORG_SUB_LOC_KEY,

case when length(trim(UPPER((IPH.PHUM)))) <1 then {{var('default_mapkey')}} when UPPER(IPH.PHUM)
IS NULL then {{var('default_mapkey')}} else UPPER(IPH.PHUM) end AS BASE_UOM_KEY,

case when length(trim(UPPER((IPH.PHCNTR)))) <1 then {{var('default_mapkey')}} when UPPER(IPH.PHCNTR)
IS NULL then {{var('default_mapkey')}} else UPPER(IPH.PHCNTR) end AS CNTNR_ID_KEY,

IPH.PHPOST AS POST_TYP,
IPH.PHID AS PHY_INVTY_DOC_NBR,
IPH.PHTAG AS PROD_TAG_NBR,
IPH.PHLOT as PROD_BATCH_NBR,
IPH.PHQTY as PROD_PHY_INVTY_QTY,
IPH.PHBONH AS BOOK_INVTY_QTY,
IPH.PHQUM AS TXN_QTY,
IPH.PHUPI AS PAL_NBR,
IPH.PHTOCW AS TOT_CNT_WGT,
IPH.PHTOBW AS TOT_BOOK_WGT

FROM {{source('INFORLX','IPH')}} IPH