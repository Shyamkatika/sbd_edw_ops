Select
UPPER(CONCAT(coalesce(DFUMAP.FROMDFULOC,''),'~',coalesce(DFUMAP.FROMDMDGROUP,''),'~' ,coalesce(DFUMAP.FROMDMDUNIT,''),'~' ,
coalesce(DFUMAP.FROMMODEL,''),'~' ,coalesce(DFUMAP.MAP,''),'~' ,coalesce(DFUMAP.TODFULOC,''),'~' ,coalesce(DFUMAP.TODMDGROUP,''),'~' ,coalesce(DFUMAP.TODMDUNIT,''),'~' ,coalesce(DFUMAP.EFF,''))) AS DMD_FORECASTING_UNIT_MAP_KEY,
'JDA' AS {{var('column_srcsyskey')}},
MD5(UPPER(CONCAT(coalesce(DFUMAP.FROMDFULOC,''),'~',coalesce(DFUMAP.FROMDMDGROUP,''),'~' ,coalesce(DFUMAP.FROMDMDUNIT,''),'~' ,
coalesce(DFUMAP.FROMMODEL,''),'~' ,coalesce(DFUMAP.MAP,''),'~' ,coalesce(DFUMAP.TODFULOC,''),'~' ,coalesce(DFUMAP.TODMDGROUP,''),'~' ,coalesce(DFUMAP.TODMDUNIT,''),'~' ,coalesce(DFUMAP.EFF,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
DFUMAP.LOADDTS AS {{var('column_z3loddtm')}},
DFUMAP.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
DFUMAP.DISC AS DFU_MAP_EXPR_DTE,
DFUMAP.EFF AS DFU_MAP_EFF_DTE,
case when length(trim(UPPER((DFUMAP.FROMDFULOC)))) <1 then {{var('default_mapkey')}} when UPPER(DFUMAP.FROMDFULOC)
IS NULL then {{var('default_mapkey')}} else UPPER(DFUMAP.FROMDFULOC) end AS FROM_LOC_KEY,
case when length(trim(UPPER((DFUMAP.TODFULOC)))) <1 then {{var('default_mapkey')}} when UPPER(DFUMAP.TODFULOC)
IS NULL then {{var('default_mapkey')}} else UPPER(DFUMAP.TODFULOC) end AS TO_LOC_KEY,
DFUMAP.FCSTTYPE AS FCST_TYP,
DFUMAP.HISTTYPE AS DFU_MODEL_HIST_TYP,
DFUMAP.LEVELNUM AS DFU_MAP_LVL_NBR,
case when length(trim(UPPER((DFUMAP.FROMDMDGROUP)))) <1 then {{var('default_mapkey')}} when UPPER(DFUMAP.FROMDMDGROUP)
IS NULL then {{var('default_mapkey')}} else UPPER(DFUMAP.FROMDMDGROUP) end AS DMD_GRP_FROM_KEY,
DFUMAP.LOCKEND AS DFU_MODEL_FROM_LOCK_END_DTE,
DFUMAP.LOCKSTART AS DFU_MODEL_FROM_LOCK_START_DTE,
DFUMAP.FACTOR AS DFU_MODEL_TO_ALLOC_FACTOR_PCT,
DFUMAP.FROMMODEL AS MODEL_FROM_NAME,
DFUMAP.TOMODEL AS MODEL_TO_NAME,
case when length(trim(UPPER((DFUMAP.TODMDGROUP)))) <1 then {{var('default_mapkey')}} when UPPER(DFUMAP.TODMDGROUP)
IS NULL then {{var('default_mapkey')}} else UPPER(DFUMAP.TODMDGROUP) end AS DMD_GRP_TO_KEY,
case when length(trim(UPPER((DFUMAP.FROMDMDUNIT)))) <1 then {{var('default_mapkey')}} when UPPER(DFUMAP.FROMDMDUNIT)
IS NULL then {{var('default_mapkey')}} else UPPER(DFUMAP.FROMDMDUNIT) end AS DMD_UNIT_FROM_KEY,
DFUMAP.MAP AS MODEL_MAP_NAME,
case when length(trim(UPPER((DFUMAP.TODMDUNIT)))) <1 then {{var('default_mapkey')}} when UPPER(DFUMAP.TODMDUNIT)
IS NULL then {{var('default_mapkey')}} else UPPER(DFUMAP.TODMDUNIT) end AS DMD_UNIT_TO_KEY
FROM {{source('JDA','DFUMAP')}} DFUMAP
