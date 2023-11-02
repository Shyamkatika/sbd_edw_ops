select
UPPER(CONCAT(coalesce(T001L.LGORT::VARCHAR,''),'~',coalesce(T001L.WERKS::VARCHAR,''))) AS INVTY_SLOC_KEY,
'SAPC11' AS {{var('column_srcsyskey')}},
MD5(UPPER(CONCAT(coalesce(T001L.LGORT::VARCHAR,''),'~',coalesce(T001L.WERKS::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
T001L.LOADDTS AS {{var('column_z3loddtm')}},
T001L.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},

case when length(trim(UPPER((T001L.KUNNR)))) <1 then {{var('default_mapkey')}} when UPPER(T001L.KUNNR)
IS NULL then {{var('default_mapkey')}} else UPPER(T001L.KUNNR) end AS CUST_KEY,

T001L.LGORT AS SLOC_ID,

T001L.LIFNR AS VEND_ID,

case when length(trim(UPPER((T001L.PARLG)))) <1 then {{var('default_mapkey')}} when UPPER(T001L.PARLG)
IS NULL then {{var('default_mapkey')}} else UPPER(T001L.PARLG) end AS PTNR_SLOC_KEY,

case when length(trim(UPPER((T001L.SPART)))) <1 then {{var('default_mapkey')}} when UPPER(T001L.SPART)
IS NULL then {{var('default_mapkey')}} else UPPER(T001L.SPART) end AS DIV_KEY,

case when length(trim(UPPER((T001L.VKORG)))) <1 then {{var('default_mapkey')}} when UPPER(T001L.VKORG)
IS NULL then {{var('default_mapkey')}} else UPPER(T001L.VKORG) end AS ORG_KEY,

case when length(trim(UPPER((T001L.VSTEL)))) <1 then {{var('default_mapkey')}} when UPPER(T001L.VSTEL)
IS NULL then {{var('default_mapkey')}} else UPPER(T001L.VSTEL) end AS SHIP_LOC_KEY,

T001L.WERKS AS PLANT_LOC_NAME,

case when length(trim(UPPER((T001L.DISKZ)))) <1 then {{var('default_mapkey')}} when UPPER(T001L.DISKZ)
IS NULL then {{var('default_mapkey')}} else UPPER(T001L.DISKZ) end AS MRP_SLOC_LKEY,

case when length(trim(UPPER((T001L.VTWEG)))) <1 then {{var('default_mapkey')}} when UPPER(T001L.VTWEG)
IS NULL then {{var('default_mapkey')}} else UPPER(T001L.VTWEG) end AS PROD_RENDERED_MTHD_LKEY,


case  when UPPER(T001L.XBUFX) ='X'  then 'Y' else 'N'  end AS FREEZING_BOOK_INVTY_BAL_FLAG,
case  when UPPER(T001L.XHUPF) ='X'  then 'Y' else 'N'  end AS HDLNG_UNIT_REQ_FLAG,
case  when UPPER(T001L.XLONG) ='X'  then 'Y' else 'N'  end AS NGTV_STK_QTY_ALLOW_FLAG,
case  when UPPER(T001L.OIG_ITRFL) ='X'  then 'Y' else 'N'  end AS TD_TRANSIT_FLAG,


T001L.LGOBE as SLOC_NAME,
T001L.XRESS as RESOURCE_SLOC,
T001L.MESBS AS MES_BUS_SYS_NAME,
T001L.MESST AS PRODTN_STORG_INVTY_TYP,
T001L.OIH_LICNO AS UNTAXED_STK_LICENSE_NBR,
T001L.XBLGO AS ACTV_STORG_AUTH_NAME,
T001L.OIB_TNKASSIGN AS TANK_ASGN_TYP

FROM {{source('SAPC11','T001L')}} T001L
WHERE T001L.MANDT={{var('sapc11mandtftr')}}