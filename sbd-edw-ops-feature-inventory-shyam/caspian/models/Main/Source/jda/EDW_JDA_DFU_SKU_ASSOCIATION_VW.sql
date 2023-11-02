select
UPPER(CONCAT(coalesce(DFUTOSKU.ITEM ::VARCHAR,''),'~',coalesce(DFUTOSKU.SKULOC ::VARCHAR,''),'~' ,coalesce(DFUTOSKU.DMDUNIT ::VARCHAR,''),'~',coalesce(DFUTOSKU.DMDGROUP::VARCHAR,''),'~',coalesce(DFUTOSKU.DFULOC ::VARCHAR,''),'~',coalesce(DFUTOSKU.EFF ::VARCHAR,''),'~',coalesce(DFUTOSKU.DISC::VARCHAR,''))) AS DFU_SKU_ASSOC_KEY,
'JDA' AS {{var('column_srcsyskey')}},
MD5(UPPER(CONCAT(coalesce(DFUTOSKU.ITEM::VARCHAR,''),'~',coalesce(DFUTOSKU.SKULOC::VARCHAR,''),'~' ,coalesce(DFUTOSKU.DMDUNIT::VARCHAR,''),'~',coalesce(DFUTOSKU.DMDGROUP::VARCHAR,''),'~',coalesce(DFUTOSKU.DFULOC::VARCHAR,''),'~',coalesce(DFUTOSKU.EFF::VARCHAR,''),'~',coalesce(DFUTOSKU.DISC::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
DFUTOSKU.LOADDTS AS {{var('column_z3loddtm')}},
DFUTOSKU.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},


case when length(trim(UPPER((DFUTOSKU.DFULOC)))) <1 then {{var('default_mapkey')}} when UPPER(DFUTOSKU.DFULOC)
IS NULL then {{var('default_mapkey')}} else UPPER(DFUTOSKU.DFULOC) end AS DFU_LOC_KEY,

case when length(trim(UPPER((DFUTOSKU.DMDGROUP)))) <1 then {{var('default_mapkey')}} when UPPER(DFUTOSKU.DMDGROUP)
IS NULL then {{var('default_mapkey')}} else UPPER(DFUTOSKU.DMDGROUP) end AS DMD_GRP_KEY,

case when length(trim(UPPER((DFUTOSKU.DMDUNIT)))) <1 then {{var('default_mapkey')}} when UPPER(DFUTOSKU.DMDUNIT)
IS NULL then {{var('default_mapkey')}} else UPPER(DFUTOSKU.DMDUNIT) end AS DMD_UNIT_KEY,

case when length(trim(UPPER((DFUTOSKU.ITEM)))) <1 then {{var('default_mapkey')}} when UPPER(DFUTOSKU.ITEM)
IS NULL then {{var('default_mapkey')}} else UPPER(DFUTOSKU.ITEM) end AS PROD_KEY,

case when length(trim(UPPER((DFUTOSKU.SKULOC)))) <1 then {{var('default_mapkey')}} when UPPER(DFUTOSKU.SKULOC)
IS NULL then {{var('default_mapkey')}} else UPPER(DFUTOSKU.SKULOC) end AS LOC_KEY,


DFUTOSKU.CONVFACTOR AS PLAN_TO_FCST_CONV_FACTOR,
DFUTOSKU.ALLOCFACTOR AS ALLOC_FACTOR,
DFUTOSKU.MODEL AS MODEL_ALGORITHM_NAME,
DFUTOSKU.FCSTTYPE AS FCST_TYP,
DFUTOSKU.HISTTYPE AS HIST_TYP,

DFUTOSKU.EFF AS MAP_EFF_DTE,
DFUTOSKU.DISC AS MAP_EXPR_DTE,

case when UPPER(DFUTOSKU.SUPERSEDESW) in ('NO','FALSE','0') or UPPER(DFUTOSKU.SUPERSEDESW) is null
or length(TRIM(DFUTOSKU.SUPERSEDESW))<1 then 'N' when UPPER(DFUTOSKU.SUPERSEDESW) in ('YES','TRUE','1')
then 'Y' else 'U' end as DFU_SUPERSEDE_FLAG

FROM {{source('JDA','DFUTOSKU')}} DFUTOSKU
