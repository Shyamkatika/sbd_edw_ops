SELECT 
UPPER(concat(COALESCE(trim(SLALLO.ITNBR)::VARCHAR,''),'~',COALESCE(trim(SLALLO.HOUSE)::VARCHAR,''),'~',COALESCE(trim(SLALLO.ORDNO)::VARCHAR,''),'~',COALESCE(trim(SLALLO.CONO)::VARCHAR,''),'~',COALESCE(trim(SLALLO.ITMSQ)::VARCHAR,''),'~',COALESCE(trim(SLALLO.RLNB)::VARCHAR,''),'~',COALESCE(trim(SLALLO.LLOCN)::VARCHAR,''))) AS INVTY_ALLOC_KEY,
'INFORXA' AS {{var('column_srcsyskey')}},
DATEADD(year,1900,try_to_date(concat(left(lpad(TRIM(SLALLO.FDATE),7,0),3),'-',substring(lpad(TRIM(SLALLO.FDATE),7,0),4,2),'-',substring(lpad(TRIM(SLALLO.FDATE),7,0),6,2)))) AS {{var('column_SRC_RCRD_CREATE_DTE')}},
NULL AS {{var('column_SRC_RCRD_CREATE_USERID')}},
DATEADD(year,1900,try_to_date(concat(left(lpad(TRIM(SLALLO.FDATE),7,0),3),'-',substring(lpad(TRIM(SLALLO.FDATE),7,0),4,2),'-',substring(lpad(TRIM(SLALLO.FDATE),7,0),6,2)))) AS {{var('column_SRC_RCRD_UPD_DTE')}},
NULL AS {{var('column_SRC_RCRD_UPD_USERID')}},
md5(UPPER(concat(COALESCE(trim(SLALLO.ITNBR)::VARCHAR,''),'~',COALESCE(trim(SLALLO.HOUSE)::VARCHAR,''),'~',COALESCE(trim(SLALLO.ORDNO)::VARCHAR,''),'~',COALESCE(trim(SLALLO.CONO)::VARCHAR,''),'~',COALESCE(trim(SLALLO.ITMSQ)::VARCHAR,''),'~',COALESCE(trim(SLALLO.RLNB)::VARCHAR,''),'~',COALESCE(trim(SLALLO.LLOCN)::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS  {{var('column_DEL_FROM_SRC_FLAG')}},                                                                               
'{{model.name}}' AS  {{var('column_ETL_INS_PID')}},                                                                  
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS  {{var('column_ETL_UPD_PID')}},                                                                  
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_UPD_DTE')}},
SLALLO.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}}, 
{{var('default_y')}} AS  {{var('column_currrecflag')}},                                                                                    
{{var('default_n')}} AS  {{var('column_orprecflag')}},
SLALLO.LOADDTS AS {{var('column_z3loddtm')}}, --
SLALLO.HOUSE as PLANT_LOC_KEY,
SLALLO.ITNBR as PROD_KEY,
SLALLO.ORDNO as REF_DOC_HDR_NBR,
SLALLO.ITMSQ as REF_DOC_LN_NBR,
SLALLO.HOUSE as LOC_KEY,
SLALLO.LBHNO as ALLOC_LOT_NBR,
SLALLO.LALQY as ALLOC_QTY,
SLALLO.QTRQA as ISS_QTY,
SLALLO.LLOCN as DFLT_SITE_LOC_KEY,
SLALLO.ORDNO as SLS_ORD_LN_KEY,
SLALLO.ITMSQ as LN_ITEM_NBR,
WHSMST.STID as ALLOC_SITE_LOC_KEY,
ITEMASUE.IEUPCD as EAN_CD_NBR_1,
ITEMASUE.IEUPCD as EAN_CD_1_NBR,
ITEMBL.MOHTQ as STK_ON_HAND_QTY,
ITEMBL.PURUM as UOM_KEY,
ITEMBL.MOHTQ as ON_HAND_QTY,
ITEMBL.PURUM as UOM_LKEY,
ITEMBL.SAFTY as SFTY_STK_QTY,
ITEMBL.WHSLC as SLOC_KEY,
ITMRVA.VALUC as ABC_CD_LKEY,
ITMRVA.UCDEF as PROD_ALLOC_COST,
ITMRVA.WEGHT as PACKED_PROD_WGT,
ITMRVA.WEGHT as PROD_WGT,
ITMRVA.ITTYP as MATL_TYP_LKEY
FROM {{ source("INFORXA", "SLALLO") }} as SLALLO
LEFT JOIN  {{ source("INFORXA", "WHSMST") }} as WHSMST  ON SLALLO.HOUSE = WHSMST.WHID
LEFT JOIN  {{ source("INFORXA", "ITEMASUE") }} as ITEMASUE ON SLALLO.ITNBR = ITEMASUE.IEITNO
LEFT JOIN  {{ source("INFORXA", "ITMRVA") }} as ITMRVA ON  SLALLO.ITNBR=ITMRVA.ITNBR  AND WHSMST.STID=ITMRVA.STID
LEFT JOIN  {{ source("INFORXA", "ITEMBL") }} as ITEMBL ON  SLALLO.ITNBR = ITEMBL.ITNBR AND SLALLO.HOUSE=ITEMBL.HOUSE 