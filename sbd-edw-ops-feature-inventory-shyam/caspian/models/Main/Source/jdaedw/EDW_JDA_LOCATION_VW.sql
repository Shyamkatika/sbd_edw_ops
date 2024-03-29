SELECT
LOC.LOC	as	LOC_KEY	,
'JDAEDW' AS {{var('column_srcsyskey')}},
MD5(UPPER(CONCAT(coalesce(LOC.LOC::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
LOC.LOADDTS AS {{var('column_z3loddtm')}},
LOC.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},
LOC.COUNTRY	as	CNTRY_CD	,
LOC.DESCR	as	LOC_DESC2	,
CASE WHEN ROUND(LOC.ENABLESW)=1 THEN 'Y' WHEN ROUND(LOC.ENABLESW)=0 THEN 'N' ELSE 'U' END
	as	LOC_OPTIMIZER_FLAG	,
--TRY_TO_DATE(LOC.FRZSTART,'YYYYMMDD')	as	FREEZING_PERD_START_DTE	,
LOC.FRZSTART as	FREEZING_PERD_START_DTE	,
LOC.LAT	as	LOC_LATITUDE	,
LOC.LOC as	LOC_ID	,
LOC.U_LOCTYPE	as	LOC_TYP_CD	,
LOC.LON	as	LOC_LONGITUDE	,
--TRY_TO_DATE(LOC.OHPOST,'YYYYMMDD')	as	ON_HAND_POST_DTE	,
LOC.OHPOST as	ON_HAND_POST_DTE	,
LOC.POSTALCODE	as	CNTRY_PSTL_CD,
LOC.SEQINTEXPORTDUR	as	SEQUENTIAL_EXPORT_DUR	,
LOC.SEQINTIMPORTDUR	as	SEQUENTIAL_IMPORT_DUR	,
--TRY_TO_DATE(LOC.SEQINTLASTEXPORTEDTOSEQ,'YYYYMMDD')	as	SEQUENTIAL_LAST_EXPORT_DUR_DTE	,
--TRY_TO_DATE(LOC.SEQINTLASTIMPORTEDFROMSEQ,'YYYYMMDD')	as	SEQUENTIAL_LAST_IMPORT_DUR_DTE	,

LOC.SEQINTLASTEXPORTEDTOSEQ	as	SEQUENTIAL_LAST_EXPORT_DUR_DTE	,
LOC.SEQINTLASTIMPORTEDFROMSEQ	as	SEQUENTIAL_LAST_IMPORT_DUR_DTE	,

LOC.U_GLOBAL_SOLVE	as	BATCH_PROC_REGN,
CASE WHEN ROUND(LOC.U_IO_EXCLUDE)=1 THEN 'Y' WHEN ROUND(LOC.U_IO_EXCLUDE)=0 THEN 'N' ELSE 'U' END	AS IO_CHK_FLAG,
LOC.U_LOCREGIONCD	as	LOC_REGN_CD	, 
CASE WHEN ROUND(LOC.U_IO_SS_EXCLUDE)=1 THEN 'Y' WHEN ROUND(LOC.U_IO_SS_EXCLUDE)=0 THEN 'N' ELSE 'U' END AS IO_SS_CHK_FLAG,
LOC.U_IO_LOCREGIONCD	as	IO_LOC_REGN_CD	,
LOC.U_PLAN_SYSTEM	as	SYSTEN_PLNR_NAME	,
LOC.U_PREV_LOCTYPE	as	PREV_LOC_TYP	,
LOC.U_SALESORG AS SLS_ORG_ID,
LOC.U_SECURITY_GRP	as	SECURITY_GRP,
LOC.U_STDCOSTCURRIND as CRNCY_STD_COST_NAME
FROM {{source('JDAEDW','LOC')}}  LOC