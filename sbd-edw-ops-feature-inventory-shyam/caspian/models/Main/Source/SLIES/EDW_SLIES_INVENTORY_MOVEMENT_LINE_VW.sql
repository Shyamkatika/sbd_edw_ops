select
UPPER(CONCAT(coalesce(matltran.site_ref::VARCHAR,''),'~',coalesce(matltran.trans_num::VARCHAR,''))) AS INVTY_MVMNT_LN_KEY,
'SLIES' AS {{var('column_srcsyskey')}},
MD5(UPPER(CONCAT(coalesce(matltran.site_ref::VARCHAR,''),'~',coalesce(matltran.trans_num::VARCHAR,'')))) AS {{var('column_rechashkey')}},
{{var('default_n')}} AS {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS {{var('column_ETL_UPD_DTE')}},
matltran.LOADDTS AS {{var('column_z3loddtm')}},
matltran.LOADDTS AS {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS {{var('column_verexpirydt')}},
{{var('default_y')}} AS {{var('column_currrecflag')}},
{{var('default_n')}} AS {{var('column_orprecflag')}},

case when length(trim(UPPER((matltran.site_ref)))) <1 then {{var('default_mapkey')}} when UPPER(matltran.site_ref)
IS NULL then {{var('default_mapkey')}} else UPPER(matltran.site_ref) end AS LOC_KEY,

case when length(trim(UPPER((matltran.item)))) <1 then {{var('default_mapkey')}} when UPPER(matltran.item)
IS NULL then {{var('default_mapkey')}} else UPPER(matltran.item) end AS PROD_KEY,



case when length(trim(UPPER((matltran.ref_type)))) <1 then {{var('default_mapkey')}} when UPPER(matltran.ref_type)
IS NULL then {{var('default_mapkey')}} else UPPER(matltran.ref_type) end AS PROD_MVMNT_REF_TYP_LKEY,

case when length(trim(UPPER((matltran.reason_code)))) <1 then {{var('default_mapkey')}} when UPPER(matltran.reason_code)
IS NULL then {{var('default_mapkey')}} else UPPER(matltran.reason_code) end AS RESN_CD_LKEY,

case when length(trim(UPPER((matltran.trans_type)))) <1 then {{var('default_mapkey')}} when UPPER(matltran.trans_type)
IS NULL then {{var('default_mapkey')}} else UPPER(matltran.trans_type) end AS TXN_TYP_LKEY,


case when UPPER(matltran.backflush) in ('NO','FALSE','0') or UPPER(matltran.backflush) is null
or length(TRIM(matltran.backflush))<1 then 'N' when UPPER(matltran.backflush) in ('YES','TRUE','1')
then 'Y' else 'U' end as BACKFLUSH_FLAG,



matltran.trans_date AS PROD_MVMNT_DTE,
matltran.ref_num AS PROD_MVMNT_REF_NBR,
matltran.ref_line_suf AS PROD_MVMNT_REF_LN_NBR,
matltran.cost AS PROD_MVMNT_COST,
matltran.wc AS WORK_CNTR_NAME,
matltran.matl_cost AS PROD_COST,
matltran.lbr_cost AS LABR_COST,
matltran.fovhd_cost AS OVER_HEAD_COST,
matltran.vovhd_cost AS VARBL_OVER_HEAD_COST,
matltran.out_cost AS ADDTN_COST,
matltran.count_sequence AS PROD_COUNTING_SEQ_NBR,
matltran.emp_num AS EMP_ID_NBR,
matltran.lot AS LOT_NBR,
matltran.cost_code AS COST_CD,
matltran.document_num AS INVTY_DOC_NBR,
matltran.trans_num AS PROD_MVMNT_DOC_NBR,
matltran.qty AS GOODS_RCPT_ISS_PROD_QTY,
matltran.loc AS FREE_TEXT,
matltran.whse AS WHSE_NBR,
matltran.ref_release AS DEST_REF_DOC_NBR

FROM {{source('SLIES','matltran')}} matltran