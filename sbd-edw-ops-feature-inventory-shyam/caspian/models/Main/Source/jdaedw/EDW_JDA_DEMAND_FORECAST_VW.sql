Select
CONCAT(coalesce(FCST.DMDUNIT::varchar,''),'~',coalesce(FCST.DMDGROUP::varchar,''),'~' ,coalesce(FCST.LOC::varchar,''),'~' ,coalesce(FCST.STARTDATE::varchar,''),'~' ,coalesce(FCST.TYPE::varchar,''),'~' ,coalesce(FCST.FCSTID::varchar,''),'~' ,coalesce(FCST.MODEL::varchar,''),'~','CURRENT') AS DMD_FCST_KEY,
'JDAEDW' AS {{var('column_srcsyskey')}},
MD5(CONCAT(coalesce(FCST.DMDUNIT::varchar,''),'~',coalesce(FCST.DMDGROUP::varchar,''),'~' ,coalesce(FCST.LOC::varchar,''),'~' ,coalesce(FCST.STARTDATE::varchar,''),'~' ,coalesce(FCST.TYPE::varchar,''),'~' ,coalesce(FCST.FCSTID::varchar,''),'~' ,coalesce(FCST.MODEL::varchar,''),'~','CURRENT')) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
FCST.LOADDTS AS {{var('column_z3loddtm')}},
FCST.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
'DRAFT'	as	DMD_FCST_CTGY,
'EA' as UOM_KEY,
case when length(trim(UPPER((FCST.DMDGROUP)))) <1 then {{var('default_mapkey')}} when UPPER(FCST.DMDGROUP) IS NULL then {{var('default_mapkey')}} else UPPER(FCST.DMDGROUP) end AS DMD_GRP_KEY,
case when length(trim(UPPER((FCST.DMDUNIT)))) <1 then {{var('default_mapkey')}} when UPPER(FCST.DMDUNIT) IS NULL then {{var('default_mapkey')}} else UPPER(FCST.DMDUNIT) end AS DMD_UNIT_KEY,
FCST.FCSTID AS FCST_ID_TYP,
case when length(trim(UPPER((FCST.LOC)))) <1 then {{var('default_mapkey')}} when UPPER(FCST.LOC) IS NULL then {{var('default_mapkey')}} else UPPER(FCST.LOC) end AS LOC_KEY,
FCST.TYPE AS FCST_TYP,
FCST.DUR/1440	as	FCST_DUR,
FCST.LEWMEANQTY	as	MEAN_FCST_QTY,
FCST.MARKETMGRVERSIONID	as	MKT_MGR_VER_NBR,
FCST.QTY	as	FCST_QTY,
FCST.MODEL	as	PLNG_MODEL_NAME,
--try_to_date(FCST.STARTDATE)	as	FCST_START_DTE,
to_timestamp(FCST.STARTDATE) as	FCST_START_DTE,
{{var('default_key')}} AS PROD_KEY,
{{var('default_key')}} AS FCST_STREAM_KEY
FROM {{source('JDAEDW','FCST')}} FCST