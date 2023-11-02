select
UPPER(CONCAT(coalesce(invsub.subnum ::VARCHAR,''),'~','JDAWMSLA1')) AS WHSE_TRNSFR_ORD_HDR_KEY,
'JDAWMS' AS {{var('column_srcsyskey')}},
MD5(UPPER(CONCAT(coalesce(invsub.subnum::VARCHAR,''),'~','JDAWMSLA1'))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
invsub.LOADDTS AS {{var('column_z3loddtm')}},
invsub.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
'JDAWMSLA1' as WMS_SCHEMA_NAME,

invsub.lodnum AS PAL_NBR,
invsub.subwgt AS SUB_INVTY_PHY_WGT,
invsub.wrkref AS WORK_REF_DOC_NBR,
TRY_TO_TIMESTAMP(invsub.adddte) AS LOD_ADD_DTE,
TRY_TO_TIMESTAMP(invsub.lstmov) AS LOD_LAST_MOVED_DTE,
TRY_TO_TIMESTAMP(invsub.lstdte) AS LOD_LAST_ACTVTY_DTE,
invsub.lst_usr_id AS UPD_BY_USER_ID,
invsub.subucc AS SUB_LOD_UCC_CD,
TRY_TO_TIMESTAMP(invsub.uccdte) AS UCC_SHIP_DTE,
invsub.subnum AS SUB_INVTY_NBR,

case when length(trim(UPPER((invsub.lstcod)))) <1 then {{var('default_mapkey')}} when UPPER(invsub.lstcod)
IS NULL then {{var('default_mapkey')}} else UPPER(invsub.lstcod) end AS LAST_ACTVTY_TYP_LKEY

FROM {{source('JDAWMSLA1','invsub')}} invsub