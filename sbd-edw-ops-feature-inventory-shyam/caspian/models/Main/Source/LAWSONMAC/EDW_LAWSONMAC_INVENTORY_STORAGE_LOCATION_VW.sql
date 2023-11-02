select
UPPER(CONCAT(coalesce(WMWMSMP.WMDIV::VARCHAR,''),'~',coalesce(WMWMSMP.WMWHSE::VARCHAR,''),'~',coalesce(WMWMSMP.WMNAME::VARCHAR,''))) AS INVTY_SLOC_KEY,
'LAWSONMAC' AS {{var('column_srcsyskey')}},
MD5(UPPER(CONCAT(coalesce(WMWMSMP.WMDIV::VARCHAR,''),'~',coalesce(WMWMSMP.WMWHSE::VARCHAR,''),'~',coalesce(WMWMSMP.WMNAME::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
WMWMSMP.LOADDTS AS {{var('column_z3loddtm')}},
WMWMSMP.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},


case when length(trim(UPPER((WMWMSMP.WMDIV)))) <1 then {{var('default_mapkey')}} when UPPER(WMWMSMP.WMDIV)
IS NULL then {{var('default_mapkey')}} else UPPER(WMWMSMP.WMDIV) end AS DIV_KEY,

WMWMSMP.WMWHSE AS SLOC_ID,
WMWMSMP.WMCITY AS CITY_NAME,
WMWMSMP.WMST AS STATE_NAME,

case when length(trim(UPPER((WMWMSMP.WMCNTY)))) <1 then {{var('default_mapkey')}} when UPPER(WMWMSMP.WMCNTY)
IS NULL then {{var('default_mapkey')}} else UPPER(WMWMSMP.WMCNTY) end AS CNTRY_KEY,


case when length(trim(UPPER((WMWMSMP.WMWPCD)))) <1 then {{var('default_mapkey')}} when UPPER(WMWMSMP.WMWPCD)
IS NULL then {{var('default_mapkey')}} else UPPER(WMWMSMP.WMWPCD) end AS WHSE_PAL_CD_LKEY,

case when length(trim(UPPER((WMWMSMP.WMWPTP)))) <1 then {{var('default_mapkey')}} when UPPER(WMWMSMP.WMWPTP)
IS NULL then {{var('default_mapkey')}} else UPPER(WMWMSMP.WMWPTP) end AS WHSE_PAL_TYP_LKEY,

case when length(trim(UPPER((WMWMSMP.WMRECS)))) <1 then {{var('default_mapkey')}} when UPPER(WMWMSMP.WMRECS)
IS NULL then {{var('default_mapkey')}} else UPPER(WMWMSMP.WMRECS) end AS WHSE_OPER_TYP_LKEY,

--WMWMSMP.WMDATP AS PHY_INVTY_LAST_DTE,
TRY_TO_DATE(REPLACE(ROUND(WMDATP),substr(ROUND(WMDATP),1,2),round(substr(ROUND(WMDATP),1,2)+1928)),'YYYYMMDD') as  PHY_INVTY_LAST_DTE,

WMWMSMP.WMPCNY AS PHY_INVTY_YR,
WMWMSMP.WMNAME as WHSE_NAME,
WMWMSMP.WMCNME as CNTCT_PRSN_NAME,
WMWMSMP.WMTITL AS WHSE_DESC_TEXT,
WMWMSMP.WMADR1 AS ADDR_LN_1_TEXT,
WMWMSMP.WMADR2 AS ADDR_LN_2_TEXT,
WMWMSMP.WMZIP AS ZIP_NBR,
WMWMSMP.WMPHON AS PHN_NBR,
WMWMSMP.WMRSTW AS REPLENISH_WHSE_TYP,
WMWMSMP.WMSQRF AS WHSE_AREA_TEXT,
WMWMSMP.WMABN AS WHSE_BIN_CNT,
WMWMSMP.WMAIT AS WHSE_PROD_CNT,
WMWMSMP.WMMNCD AS MNFST_DOC_NBR,
WMWMSMP.WMOUT1 AS OBND_SHIP_DTL_1_TEXT,
WMWMSMP.WMOUT2 AS OBND_SHIP_DTL_2_TEXT,
WMWMSMP.WMOUT3 AS OBND_SHIP_DTL_3_TEXT,
WMWMSMP.WMOUT4 AS OBND_SHIP_DTL_4_TEXT,
WMWMSMP.WMOUT5 AS OBND_SHIP_DTL_5_TEXT

FROM {{source('LAWSONMAC','WMWMSMP')}} WMWMSMP

