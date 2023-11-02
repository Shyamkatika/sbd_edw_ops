select 
UPPER(CONCAT((cnttyp.cnttyp::VARCHAR),'~','JDAWMSEANZ')) as CYCLE_CNT_CLS_KEY,
'JDAWMS' AS  {{var('column_srcsyskey')}},
MD5(UPPER(CONCAT((cnttyp.cnttyp::VARCHAR),'~','JDAWMSEANZ'))) AS  {{var('column_rechashkey')}},
{{var('default_n')}} as {{var('column_DEL_FROM_SRC_FLAG')}},
'{{model.name}}' AS  {{var('column_ETL_INS_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_INS_DTE')}},
'{{model.name}}' AS  {{var('column_ETL_UPD_PID')}},
CURRENT_TIMESTAMP::TIMESTAMP_NTZ AS  {{var('column_ETL_UPD_DTE')}},
cnttyp.LOADDTS AS  {{var('column_vereffdte')}},
'9999-12-31 00:00:00.000' AS  {{var('column_verexpirydt')}},
{{var('default_y')}} as {{var('column_currrecflag')}},
{{var('default_n')}} as {{var('column_orprecflag')}},
cnttyp.LOADDTS AS  {{var('column_z3loddtm')}},
'JDAWMSEANZ' as WMS_SCHEMA_NAME,
case when length(trim(UPPER((cnttyp.cnttyp)))) <1 then {{var('default_mapkey')}} when UPPER(cnttyp.cnttyp)
= 'NULL' then {{var('default_mapkey')}} else UPPER(cnttyp.cnttyp) end AS CYCLE_CNT_TYP_LKEY,
case when length(trim(UPPER((cnttyp.oprcod)))) <1 then {{var('default_mapkey')}} when UPPER(cnttyp.oprcod)
= 'NULL' then {{var('default_mapkey')}} else UPPER(cnttyp.oprcod) end AS OPER_CD_TYP_LKEY,
case when length(trim(UPPER((cnttyp.lpncnt_oprcod)))) <1 then {{var('default_mapkey')}} when UPPER(cnttyp.lpncnt_oprcod)
= 'NULL' then {{var('default_mapkey')}} else UPPER(cnttyp.lpncnt_oprcod) end AS LPN_CNT_OPER_TYP_LKEY,
case when UPPER(cnttyp.dtl_flg) in ('NO','FALSE','0') or UPPER(cnttyp.dtl_flg) is null
or length(TRIM(cnttyp.dtl_flg))<1 then 'N' when UPPER(cnttyp.dtl_flg) in ('YES','TRUE','1')
then 'Y' else 'U' end AS SUMM_OR_DTL_CNT_FLAG,
case when length(trim(UPPER((cnttyp.end_loc_cnt)))) <1 then {{var('default_mapkey')}} when UPPER(cnttyp.end_loc_cnt)
= 'NULL' then {{var('default_mapkey')}} else UPPER(cnttyp.end_loc_cnt) end as LOC_CNT_ENDED_LKEY,
case when length(trim(UPPER((cnttyp.nxt_cnttyp)))) <1 then {{var('default_mapkey')}} when UPPER(cnttyp.nxt_cnttyp)
= 'NULL' then {{var('default_mapkey')}} else UPPER(cnttyp.nxt_cnttyp) end AS NEXT_CNT_TYP_LKEY,
case when UPPER(cnttyp.sum_retry_flg) in ('NO','FALSE','0') or UPPER(cnttyp.sum_retry_flg) is null
or length(TRIM(cnttyp.sum_retry_flg))<1 then 'N' when UPPER(cnttyp.sum_retry_flg) in ('YES','TRUE','1')
then 'Y' else 'U' end as SUMM_RETRY_INDCTR_FLAG,
cnttyp.cls_loc_cnt_mthd as LOC_CLOSE_TYP,
case when UPPER(cnttyp.emp_loc_flg) in ('NO','FALSE','0') or UPPER(cnttyp.emp_loc_flg) is null
or length(TRIM(cnttyp.emp_loc_flg))<1 then 'N' when UPPER(cnttyp.emp_loc_flg) in ('YES','TRUE','1')
then 'Y' else 'U' end as EMPTY_LOC_CNT_READ_INDCTR_FLAG,
case when length(trim(UPPER((cnttyp.adj_mthd)))) <1 then {{var('default_mapkey')}} when UPPER(cnttyp.adj_mthd)
= 'NULL' then {{var('default_mapkey')}} else UPPER(cnttyp.adj_mthd) end AS INVTY_ADJ_MTHD_TYP_LKEY,
case when UPPER(cnttyp.homog_adj_flg) in ('NO','FALSE','0') or UPPER(cnttyp.homog_adj_flg) is null
or length(TRIM(cnttyp.homog_adj_flg))<1 then 'N' when UPPER(cnttyp.homog_adj_flg) in ('YES','TRUE','1')
then 'Y' else 'U' end as ADJUSTMENTS_ALLOW_INDCTR_FLAG,
case when UPPER(cnttyp.rf_prmpt_rea_flg) in ('NO','FALSE','0') or UPPER(cnttyp.rf_prmpt_rea_flg) is null
or length(TRIM(cnttyp.rf_prmpt_rea_flg))<1 then 'N' when UPPER(cnttyp.rf_prmpt_rea_flg) in ('YES','TRUE','1')
then 'Y' else 'U' end as SUMM_ADJ_CNT_FLAG,
TRY_TO_TIMESTAMP(cnttyp.ins_dt) AS SUMM_ADJ_RESN_CD_DTE,
case when UPPER(cnttyp.diff_usr_flg) in ('NO','FALSE','0') or UPPER(cnttyp.diff_usr_flg) is null
or length(TRIM(cnttyp.diff_usr_flg))<1 then 'N' when UPPER(cnttyp.diff_usr_flg) in ('YES','TRUE','1')
then 'Y' else 'U' end as SAME_USER_ALLOW_INDCTR_FLAG
FROM {{source('JDAWMSEANZ','CNTTYP')}} CNTTYP