select
CONCAT(coalesce(HIST.DMDUNIT::VARCHAR,''),'~',coalesce(HIST.DMDGROUP::VARCHAR,''),'~',coalesce(HIST.LOC::VARCHAR,''),'~',coalesce(HIST.HISTSTREAM::VARCHAR,''),'~',coalesce(HIST.STARTDATE::VARCHAR,''),'~',coalesce(HIST.TYPE::VARCHAR,''),'~',coalesce(HIST.EVENT::VARCHAR,'')) AS DMD_SLS_HIST_KEY,
'JDA' AS {{var('column_srcsyskey')}},
MD5(CONCAT(coalesce(HIST.DMDUNIT::VARCHAR,''),'~',coalesce(HIST.DMDGROUP::VARCHAR,''),'~',coalesce(HIST.LOC::VARCHAR,''),'~',coalesce(HIST.HISTSTREAM::VARCHAR,''),'~',coalesce(HIST.STARTDATE::VARCHAR,''),'~',coalesce(HIST.TYPE::VARCHAR,''),'~',coalesce(HIST.EVENT::VARCHAR,''))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
HIST.LOADDTS AS {{var('column_z3loddtm')}},
HIST.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
'EA' AS UOM_TYP,
case when length(trim(UPPER((HIST.LOC)))) <1 then {{var('default_mapkey')}} when UPPER(HIST.LOC)
IS NULL then {{var('default_mapkey')}} else UPPER(HIST.LOC) end AS LOC_KEY,

case when length(trim(UPPER((HIST.DMDGROUP)))) <1 then {{var('default_mapkey')}} when UPPER(HIST.DMDGROUP)
IS NULL then {{var('default_mapkey')}} else UPPER(HIST.DMDGROUP) end AS DMD_GRP_KEY,

case when length(trim(UPPER((HIST.DMDUNIT)))) <1 then {{var('default_mapkey')}} when UPPER(HIST.DMDUNIT)
IS NULL then {{var('default_mapkey')}} else UPPER(HIST.DMDUNIT) end AS DMD_UNIT_KEY,

HIST.DUR AS DUR_PERD_WK_CNT,
HIST.EVENT AS OVRD_EVNT_NAME,
HIST.HISTSTREAM AS HIST_STREAM_TEXT,
HIST.QTY AS HIST_FCST_QTY,


HIST.STARTDATE AS HIST_START_DTE,

HIST.TYPE AS DMD_HIST_TYP,

HIST.U_EXT_GSV AS EXTND_GSV_VAL,
HIST.U_RETAIL_VAL AS U_RETAIL_VAL,
COALESCE(DMDGROUP.U_LOCALCURRENCY,'USD') AS CRNCY_TYP

FROM {{source('JDA','HIST')}} HIST
LEFT JOIN  {{source('JDA','DMDGROUP')}} DMDGROUP
    ON HIST.DMDGROUP = DMDGROUP.DMDGROUP

